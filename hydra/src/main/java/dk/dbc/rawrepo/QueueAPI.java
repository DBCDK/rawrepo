/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.dao.HoldingsItemsConnector;
import dk.dbc.rawrepo.dao.OpenAgencyConnector;
import dk.dbc.rawrepo.dao.RawRepoConnector;
import dk.dbc.rawrepo.json.QueueException;
import dk.dbc.rawrepo.json.QueueProvider;
import dk.dbc.rawrepo.json.QueueValidateRequest;
import dk.dbc.rawrepo.json.QueueType;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import net.logstash.logback.encoder.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.json.*;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Pattern;


@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_QUEUE)
public class QueueAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueAPI.class);

    private static final String MESSAGE_NO_RECORDS = "Der blev ikke fundet nogen poster, så intet er lagt på kø";
    private static final String MESSAGE_FAIL_INVALID_AGENCY_FORMAT = "Værdien \"%s\" har ikke et gyldigt format for et biblioteksnummer";
    private static final String MESSAGE_FAIL_INVAILD_AGENCY_ID = "Biblioteksnummeret %s tilhører ikke biblioteksgruppen %s";
    private static final String MESSAGE_FAIL_QUEUETYPE = "Køtypen \"%s\" kunne ikke valideres";
    private static final String MESSAGE_FAIL_PROVIDER = "Provideren %s kunne ikke valideres";
    private static final String MESSAGE_FAIL_AGENCY_MISSING = "Der skal angives mindst ét biblioteksnummer";

    private static final Integer CHUNK_SIZE = 5000;

    @EJB
    private OpenAgencyConnector openAgency;

    @EJB
    private RawRepoConnector rawrepo;

    @EJB
    private HoldingsItemsConnector holdingsItemsConnector;

    private PassiveExpiringMap<String, List<RecordId>> chunkCache = new PassiveExpiringMap<>(1000 * 60 * 60 * 24); // 24 hours

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_PROVIDERS)
    public Response getProviders() {
        LOGGER.entry();
        String res = "";

        JsonObjectBuilder outerJSON = Json.createObjectBuilder();
        JsonArrayBuilder innerJSON = Json.createArrayBuilder();

        try {
            List<String> providers = rawrepo.getProviders();

            for (String provider : providers) {
                innerJSON.add(provider);
            }

            outerJSON.add("providers", innerJSON);
            JsonObject jsonObject = outerJSON.build();
            res = jsonObject.toString();
            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    @Stopwatch
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_VALIDATE)
    public Response validate(String inputStr) {
        LOGGER.info("Validate request: {}", inputStr);
        String res = "";
        String sessionId = UUID.randomUUID().toString();

        try {
            QueueValidateRequest queueValidateRequest = parseQueueInput(inputStr);

            Set<RecordId> recordIdSet = getRecordIdsForQueuing(queueValidateRequest);

            Map<Integer, Integer> recordCount = new HashMap<>();

            for (RecordId recordId : recordIdSet) {
                Integer agencyId = recordId.getAgencyId();
                Integer count = 1;

                if (recordCount.containsKey(agencyId)) {
                    count = recordCount.get(agencyId);
                    count++;
                }

                recordCount.put(agencyId, count);
            }

            List<RecordId> recordIdList = new ArrayList<>();
            recordIdList.addAll(recordIdSet);
            this.chunkCache.put(sessionId, recordIdList);

            JsonObjectBuilder responseJSON = Json.createObjectBuilder();
            JsonArrayBuilder analysisList = Json.createArrayBuilder();

            for (Integer key : recordCount.keySet()) {
                JsonObjectBuilder innerAnalysis = Json.createObjectBuilder();
                innerAnalysis.add("agencyId", key);
                innerAnalysis.add("count", recordCount.get(key));

                analysisList.add(innerAnalysis);
            }
            responseJSON.add("analysis", analysisList);
            responseJSON.add("chunks", (recordIdSet.size() / CHUNK_SIZE));
            responseJSON.add("sessionId", sessionId);
            responseJSON.add("validated", true);

            res = responseJSON.build().toString();

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException e) {
            return constructResponse(false, e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected exception:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private Set<RecordId> getRecordIdsForQueuing(QueueValidateRequest queueValidateRequest) throws Exception {
        Set<RecordId> holdingsItemsRecordIds, fbsRecordIds, recordMap = null;
        QueueType queueType = queueValidateRequest.getQueueType();
        Set<Integer> agencyList = queueValidateRequest.getAgencyIds();
        boolean includeDeleted = queueValidateRequest.isIncludeDeleted();

        switch (queueType.getKey()) {
            case QueueType.KEY_DBC_COMMON_ONLY:
                recordMap = rawrepo.getRecordsForDBC(agencyList, includeDeleted);
                break;
            case QueueType.KEY_FFU:
            case QueueType.KEY_FBS_RR:
                recordMap = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                break;
            case QueueType.KEY_FBS_RR_ENRICHEMENT:
                recordMap = rawrepo.getEnrichmentForAgencies(agencyList, includeDeleted);
                break;
            case QueueType.KEY_FBS_HOLDINGS:
                holdingsItemsRecordIds = holdingsItemsConnector.getHoldingsRecords(agencyList);
                fbsRecordIds = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                recordMap = convertNotExistingRecordIds(holdingsItemsRecordIds, fbsRecordIds);
                break;
            case QueueType.KEY_FBS_EVERYTHING:
                holdingsItemsRecordIds = holdingsItemsConnector.getHoldingsRecords(agencyList);
                fbsRecordIds = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                recordMap = convertNotExistingRecordIds(holdingsItemsRecordIds, fbsRecordIds);
                // Rawrepo might contain records which the library doesn't have holdings on, so we have to add
                // the FBS records to the list as well
                recordMap.addAll(fbsRecordIds);
                break;
        }

        return recordMap;
    }

    @Stopwatch
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_PROCESS)
    public Response process(String inputStr) {
        String res = "";

        try {
            QueueValidateRequest queueValidateRequest = parseQueueInput(inputStr);

            String provider = queueValidateRequest.getProvider();
            QueueType queueType = queueValidateRequest.getQueueType();
            String sessionId = queueValidateRequest.getSessionId();
            Integer chunkIndex = queueValidateRequest.getChunk();

            List<RecordId> sessionCache = this.chunkCache.get(sessionId);
            List<RecordId> chunk = sessionCache.subList(Math.max(0, chunkIndex * CHUNK_SIZE),
                    Math.min(sessionCache.size(), (chunkIndex + 1) * CHUNK_SIZE));

            List<String> bibliographicRecordIdList = new ArrayList<>();
            List<Integer> agencyListBulk = new ArrayList<>();
            List<String> providerList = new ArrayList<>();
            List<Boolean> changedList = new ArrayList<>();
            List<Boolean> leafList = new ArrayList<>();
            if (chunk.size() > 0) {
                LOGGER.debug("{} records will be enqueued", chunk.size());

                for (RecordId recordId : chunk) {
                    bibliographicRecordIdList.add(recordId.getBibliographicRecordId());
                    agencyListBulk.add(recordId.getAgencyId());
                    providerList.add(provider);
                    changedList.add(queueType.isChanged());
                    leafList.add(queueType.isLeaf());
                }

                rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

                return constructResponse(true, "");
            } else {
                return constructResponse(true, MESSAGE_NO_RECORDS);
            }
        } catch (QueueException e) {
            return constructResponse(false, e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected exception:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private QueueValidateRequest parseQueueInput(String inputStr) throws Exception {
        QueueValidateRequest result = new QueueValidateRequest();

        try {
            JsonReader reader = Json.createReader(new StringReader(inputStr));
            JsonObject obj = reader.readObject();

            List<String> allowedProviders = rawrepo.getProviders();

            String provider = obj.getString("provider");
            if (provider == null || provider.isEmpty() || !allowedProviders.contains(provider)) {
                throw new QueueException(String.format(MESSAGE_FAIL_PROVIDER, provider));
            }
            result.setProvider(provider);

            QueueType queueType = QueueType.fromString(obj.getString("queueType"));
            if (queueType == null) {
                throw new QueueException(String.format(MESSAGE_FAIL_QUEUETYPE, obj.getString("queueType")));
            }
            result.setQueueType(queueType);

            boolean includeDeleted = Boolean.parseBoolean(obj.getString("includeDeleted"));
            result.setIncludeDeleted(includeDeleted);

            String agencyString = obj.getString("agencyText");
            agencyString = agencyString.replace("\n", ","); // Transform newline and space separation into comma separation
            agencyString = agencyString.replace(" ", ",");
            List<String> agencies = Arrays.asList(agencyString.split(","));
            Set<String> allowedAgencies = openAgency.getLibrariesByCatalogingTemplateSet(queueType.getCatalogingTemplateSet());

            List<String> cleanedAgencyList = new ArrayList<>();
            for (String agency : agencies) {
                String cleanedAgency = agency.trim();
                if (!cleanedAgency.isEmpty()) {
                    cleanedAgencyList.add(cleanedAgency);
                }
            }

            if (cleanedAgencyList.size() == 0) {
                throw new QueueException(MESSAGE_FAIL_AGENCY_MISSING);
            }

            Set<Integer> agencyList = new HashSet<>();
            final Pattern p = Pattern.compile("(\\d{6})"); // Digit, length of 6
            for (String agency : cleanedAgencyList) {
                if (!p.matcher(agency).find()) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVALID_AGENCY_FORMAT, agency));
                }
                if (!allowedAgencies.containsAll(Collections.singleton(agency))) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVAILD_AGENCY_ID, agency, queueType.getCatalogingTemplateSet()));
                }
                agencyList.add(Integer.parseInt(agency));
            }
            result.setAgencyIds(agencyList);

            if (obj.containsKey("chunk")) {
                result.setChunk(obj.getInt("chunk"));
            }

            if (obj.containsKey("sessionId")) {
                result.setSessionId(obj.getString("sessionId"));
            }

            LOGGER.info(result.toString());

            return result;
        } finally {
            LOGGER.exit(result);
        }
    }

    /**
     * This function is necessary due to how Corepo works.
     * Corepo only contains real/existing record. However holdings items will often return ids of records
     * that doesn't actually exist. In those case we have to override the agencyId to be that of 191919 instead
     * so that Corepo receives the correct register
     *
     * @param holdingsItemsRecordIds Set of record ids from holdings items
     * @param fbsRecordIds           Set of record ids from rawrepo
     * @return Set of record ids
     */
    private Set<RecordId> convertNotExistingRecordIds(Set<RecordId> holdingsItemsRecordIds, Set<RecordId> fbsRecordIds) {
        Set<RecordId> result = new HashSet<>();

        for (RecordId holdingsItemsRecordId : holdingsItemsRecordIds) {
            if (fbsRecordIds.contains(holdingsItemsRecordId)) {
                result.add(holdingsItemsRecordId);
            } else {
                result.add(new RecordId(holdingsItemsRecordId.bibliographicRecordId, 191919));
            }
        }

        return result;
    }

    private Response constructResponse(boolean validated, String message) {
        JsonObjectBuilder responseJSON = Json.createObjectBuilder();
        responseJSON.add("validated", validated);
        responseJSON.add("message", message);

        String res = responseJSON.build().toString();

        if (!validated) {
            LOGGER.error("Response: " + message);
        }

        return Response.ok(res, MediaType.APPLICATION_JSON).build();
    }

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_PROVIDER_INFO)
    public Response getProviderInfo() {
        LOGGER.entry();
        String res = "";

        try {
            List<QueueProvider> providers = rawrepo.getProviderDetails();
            LOGGER.info(providers.toString());

            HashMap<String, List<QueueProvider>> resultObject = new HashMap<>();
            resultObject.putIfAbsent("providers", providers);

            ObjectMapper mapper = new ObjectMapper();
            res = mapper.writeValueAsString(resultObject);
            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getProviderInfo", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_TYPES)
    public Response getCatalogingTemplateSets() {
        LOGGER.entry();

        List<QueueType> queueTypes = this.getQueueTypes();

        String res = "";
        try {
            JsonObjectBuilder outerJSON = Json.createObjectBuilder();
            JsonArrayBuilder innerJSON = Json.createArrayBuilder();

            for (QueueType queueType : queueTypes) {
                JsonObjectBuilder value = Json.createObjectBuilder();
                value.add("key", queueType.getKey());
                value.add("description", queueType.getDescription());
                innerJSON.add(value);
            }
            outerJSON.add("values", innerJSON);
            JsonObject jsonObject = outerJSON.build();
            res = jsonObject.toString();
            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private List<QueueType> getQueueTypes() {
        List<QueueType> result = new ArrayList<>();
        result.add(QueueType.ffu());
        result.add(QueueType.fbsRawrepo());
        result.add(QueueType.fbsRawrepoEnrichment());
        result.add(QueueType.fbsHoldings());
        result.add(QueueType.fbsEverything());

        // Hack to only enable DBC queue type on basismig environment
        if (System.getenv("INSTANCE_NAME").toLowerCase().contains("basismig")) {
            result.add(QueueType.dbcCommon());
        }

        return result;
    }

}
