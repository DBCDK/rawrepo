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
import dk.dbc.rawrepo.json.QueueRequest;
import dk.dbc.rawrepo.json.QueueType;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import net.logstash.logback.encoder.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_QUEUE)
public class QueueAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueAPI.class);

    private static final String MESSAGE_SUCCESS = "I alt %s post(er) blev fundet og er nu lagt på kø";
    private static final String MESSAGE_NO_RECORDS = "Der blev ikke fundet nogen poster, så intet er lagt på kø";
    private static final String MESSAGE_FAIL_INVALID_AGENCY_FORMAT = "Værdien \"%s\" har ikke et gyldigt format for et biblioteksnummer";
    private static final String MESSAGE_FAIL_INVAILD_AGENCY_ID = "Biblioteksnummeret %s tilhører ikke biblioteksgruppen %s";
    private static final String MESSAGE_FAIL_QUEUETYPE = "Køtypen \"%s\" kunne ikke valideres";
    private static final String MESSAGE_FAIL_PROVIDER = "Provideren %s kunne ikke valideres";
    private static final String MESSAGE_FAIL_AGENCY_MISSING = "Der skal angives mindst ét biblioteksnummer";

    @EJB
    private OpenAgencyConnector openAgency;

    @EJB
    private RawRepoConnector rawrepo;

    @EJB
    private HoldingsItemsConnector holdingsItemsConnector;

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

    private String recordIdsToBase64(Set<RecordId> recordIds) {
        StringBuilder sb = new StringBuilder();
        for (RecordId recordId : recordIds) {
            sb.append(recordId.getBibliographicRecordId());
            sb.append(":");
            sb.append(Integer.toString(recordId.getAgencyId()));
            sb.append(";");
        }

        String chunkString = sb.toString();

        byte[] encodedBytes = Base64.getEncoder().encode(chunkString.getBytes());
        return new String(encodedBytes);
    }

    private Set<RecordId> base64ToRecordIds(String base) {
        Set<RecordId> result = new HashSet<>();

        byte[] decoded = Base64.getDecoder().decode(base);
        String decodedString = new String(decoded);

        String[] split = decodedString.split(";");
        for (String s : split) {
            String[] splitRecordId = s.split(":");
            RecordId recordId = new RecordId(splitRecordId[0], Integer.parseInt(splitRecordId[1]));
            result.add(recordId);
        }

        return result;
    }


    @Stopwatch
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_QUEUE_VALIDATE)
    public Response validate(String inputStr) {
        String res = "";

        try {
            QueueRequest queueRequest = parseQueueInput(inputStr);

            Set<RecordId> recordMap = getRecordIdsForQueuing(queueRequest);

            Map<Integer, Integer> recordCount = new HashMap<>();

            //Set<RecordId> chunk = new HashSet<>();
            Map<Integer, Set<RecordId>> chunks = new HashMap<>();
            Integer chunkIndex = 0;

            for (RecordId recordId : recordMap) {
                if (!chunks.containsKey(chunkIndex)) {
                    chunks.put(chunkIndex, new HashSet<>());
                }
                chunks.get(chunkIndex).add(recordId);
                if (chunks.get(chunkIndex).size() == 1000) {
                    chunkIndex++;
                }

                Integer agencyId = recordId.getAgencyId();
                Integer count = 1;

                if (recordCount.containsKey(agencyId)) {
                    count = recordCount.get(agencyId);
                    count++;
                }

                recordCount.put(agencyId, count);
            }

            JsonObjectBuilder responseJSON = Json.createObjectBuilder();
            JsonArrayBuilder analysisList = Json.createArrayBuilder();

            for (Integer key : recordCount.keySet()) {
                JsonObjectBuilder innerAnalysis = Json.createObjectBuilder();
                innerAnalysis.add("agencyId", key);
                innerAnalysis.add("count", recordCount.get(key));

                analysisList.add(innerAnalysis);
            }
            responseJSON.add("analysis", analysisList);

            JsonArrayBuilder chunkList = Json.createArrayBuilder();
            for (int i = 0; i <= chunkIndex; i++) {
                chunkList.add(recordIdsToBase64(chunks.get(i)));
            }
            responseJSON.add("chunks", chunkList);
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

    private Set<RecordId> getRecordIdsForQueuing(QueueRequest queueRequest) throws Exception {
        Set<RecordId> holdingsItemsRecordIds, fbsRecordIds, recordMap = null;
        QueueType queueType = queueRequest.getQueueType();
        Set<Integer> agencyList = queueRequest.getAgencyIds();
        boolean includeDeleted = queueRequest.isIncludeDeleted();

        switch (queueType.getKey()) {
            case QueueType.KEY_FFU:
                recordMap = rawrepo.getRecordsForAgencies(agencyList, includeDeleted);
                break;
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

            QueueRequest queueRequest = parseQueueInput(inputStr);
            String provider = queueRequest.getProvider();
            QueueType queueType = queueRequest.getQueueType();
            Set<Integer> agencyList = queueRequest.getAgencyIds();

            LOGGER.debug("A total of {} agencies was found in input. Looking for records...", agencyList.size());

            //Set<RecordId> recordMap = getRecordIdsForQueuing(queueRequest);
            /*Set<RecordId> recordMap = new HashSet<>();
            for (String chunk : queueRequest.getChunks()) {
                recordMap.addAll(base64ToRecordIds(chunk));
            }
*/
            Set<RecordId> recordMap = base64ToRecordIds(queueRequest.getChunk());

            List<String> bibliographicRecordIdList = new ArrayList<>();
            List<Integer> agencyListBulk = new ArrayList<>();
            List<String> providerList = new ArrayList<>();
            List<Boolean> changedList = new ArrayList<>();
            List<Boolean> leafList = new ArrayList<>();
            if (recordMap.size() > 0) {
                for (RecordId recordId : recordMap) {
                    bibliographicRecordIdList.add(recordId.bibliographicRecordId);
                    agencyListBulk.add(recordId.agencyId);
                    providerList.add(provider);
                    changedList.add(queueType.isChanged());
                    leafList.add(queueType.isLeaf());
                }

                LOGGER.debug("{} records will be enqueued", recordMap.size());

                rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

                return constructResponse(true, String.format(MESSAGE_SUCCESS, recordMap.size()));
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

    private QueueRequest parseQueueInput(String inputStr) throws Exception {
        //LOGGER.info("Input string: {}", inputStr);
        QueueRequest result = new QueueRequest();

        try {
            JsonReader reader = Json.createReader(new StringReader(inputStr));
            JsonObject obj = reader.readObject();

            List<String> allowedProviders = rawrepo.getProviders();

            String provider = obj.getString("provider");
            if (provider == null || provider.isEmpty() || !allowedProviders.contains(provider)) {
                throw new QueueException(String.format(MESSAGE_FAIL_PROVIDER, provider));
            }
            result.setProvider(provider);
            LOGGER.debug("provider: {}", provider);

            QueueType queueType = QueueType.fromString(obj.getString("queueType"));
            if (queueType == null) {
                throw new QueueException(String.format(MESSAGE_FAIL_QUEUETYPE, obj.getString("queueType")));
            }
            result.setQueueType(queueType);
            LOGGER.debug("QueueType: {}", queueType);

            boolean includeDeleted = Boolean.parseBoolean(obj.getString("includeDeleted"));
            result.setIncludeDeleted(includeDeleted);
            LOGGER.debug("includeDeleted: {}", includeDeleted);

            String agencyString = obj.getString("agencyText");
            LOGGER.debug("agencyString: {}", agencyString);
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
            final Pattern p = Pattern.compile("(\\d{6})"); // Digit, 6 chars
            for (String agency : cleanedAgencyList) {
                if (!p.matcher(agency).find()) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVALID_AGENCY_FORMAT, agency));
                }
                if (!allowedAgencies.containsAll(Collections.singleton(agency))) {
                    throw new QueueException(String.format(MESSAGE_FAIL_INVAILD_AGENCY_ID, agency, queueType.getCatalogingTemplateSet()));
                }
                LOGGER.debug("Found agency {} in input agency string", agency);
                agencyList.add(Integer.parseInt(agency));
            }
            result.setAgencyIds(agencyList);

            if (obj.containsKey("chunk")) {
                result.setChunk(obj.getString("chunk"));
            }

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

        if (validated) {
            LOGGER.info("Response: " + message);
        } else {
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

}
