/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.dao.HoldingsItemsDAO;
import dk.dbc.rawrepo.dao.OpenAgencyDAO;
import dk.dbc.rawrepo.dao.RawRepoDAO;
import dk.dbc.rawrepo.json.QueueProvider;
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
import java.util.regex.Pattern;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_QUEUE)
public class QueueAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueAPI.class);

    private static final String MESSAGE_SUCCESS = "I alt %s post(er) blev fundet og er nu lagt på kø";
    private static final String MESSAGE_FAIL_INVALID_AGENCY_FORMAT = "Værdien \"%s\" har ikke et gyldigt format for et biblioteksnummer";
    private static final String MESSAGE_FAIL_INVAILD_AGENCY_ID = "Biblioteksnummeret %s tilhører ikke biblioteksgruppen %s";
    private static final String MESSAGE_FAIL_QUEUETYPE = "Køtypen \"%s\" kunne ikke valideres";
    private static final String MESSAGE_FAIL_PROVIDER = "Provideren %s kunne ikke valideres";
    private static final String MESSAGE_FAIL_AGENCY_MISSING = "Der skal angives mindst ét biblioteksnummer";

    @EJB
    private OpenAgencyDAO openAgency;

    @EJB
    private RawRepoDAO rawrepo;

    @EJB
    private HoldingsItemsDAO holdingsItemsDAO;

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
    @Path(ApplicationConstants.API_QUEUE_ENQUEUE)
    public Response enqueue(String inputStr) {
        LOGGER.info("Input string: {}", inputStr);
        String res = "";

        try {
            JsonReader reader = Json.createReader(new StringReader(inputStr));
            JsonObject obj = reader.readObject();

            List<String> allowedProviders = rawrepo.getProviders();

            String provider = obj.getString("provider");
            if (provider == null || provider.isEmpty() || !allowedProviders.contains(provider)) {
                return constructResponse(false, String.format(MESSAGE_FAIL_PROVIDER, provider));
            }
            LOGGER.debug("provider: {}", provider);

            QueueType queueType = QueueType.fromString(obj.getString("queueType"));
            if (queueType == null) {
                return constructResponse(false, String.format(MESSAGE_FAIL_QUEUETYPE, obj.getString("queueType")));
            }
            LOGGER.debug("QueueType: {}", queueType);

            boolean includeDeleted = obj.getBoolean("includeDeleted");
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
                return constructResponse(false, MESSAGE_FAIL_AGENCY_MISSING);
            }

            List<Integer> agencyList = new ArrayList<>();
            final Pattern p = Pattern.compile("(\\d{6})");
            for (String agency : cleanedAgencyList) {
                if (!p.matcher(agency).find()) {
                    return constructResponse(false, String.format(MESSAGE_FAIL_INVALID_AGENCY_FORMAT, agency));
                }
                if (!allowedAgencies.containsAll(Collections.singleton(agency))) {
                    return constructResponse(false, String.format(MESSAGE_FAIL_INVAILD_AGENCY_ID, agency, queueType.getCatalogingTemplateSet()));
                }
                LOGGER.debug("Found agency {} in input agency string", agency);
                agencyList.add(Integer.parseInt(agency));
            }

            LOGGER.debug("A total of {} agencies was found in input. Looking for records...", agencyList.size());
            HashMap<Integer, Set<String>> recordMap = null;
            if (queueType.getKey().equals(QueueType.KEY_FFU)) {
                recordMap = rawrepo.getFFURecords(agencyList, includeDeleted);
            } else if (queueType.getKey().equals(QueueType.KEY_FBS_RR)) {
                recordMap = rawrepo.getFBSRecords(agencyList, includeDeleted);
            } else if (queueType.getKey().equals(QueueType.KEY_FBS_HOLDINGS)) {
                recordMap = holdingsItemsDAO.getHoldingsRecords(agencyList);
            } else if (queueType.getKey().equals(QueueType.KEY_FBS_EVERYTHING)) {
                HashMap<Integer, Set<String>> mapA = holdingsItemsDAO.getHoldingsRecords(agencyList);
                HashMap<Integer, Set<String>> mapB = rawrepo.getFBSRecords(agencyList, includeDeleted);

                recordMap = mergeHashMaps(mapA, mapB);
            }

            List<String> bibliographicRecordIdList = new ArrayList<>();
            List<Integer> agencyListBulk = new ArrayList<>();
            List<String> providerList = new ArrayList<>();
            List<Boolean> changedList = new ArrayList<>();
            List<Boolean> leafList = new ArrayList<>();

            for (Integer agencyId : recordMap.keySet()) {
                LOGGER.debug("Found {} records for agency {}", recordMap.get(agencyId).size(), agencyId);
                for (String bibliographicRecordId : recordMap.get(agencyId)) {
                    bibliographicRecordIdList.add(bibliographicRecordId);
                    agencyListBulk.add(agencyId);
                    providerList.add(provider);
                    changedList.add(queueType.isChanged());
                    leafList.add(queueType.isLeaf());
                }
            }

            LOGGER.debug("{} records will be enqueued", recordMap.size());

            rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

            return constructResponse(true, String.format(MESSAGE_SUCCESS, recordMap.size()));
        } catch (Exception e) {
            LOGGER.error("Something happened:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private HashMap<Integer, Set<String>> mergeHashMaps(HashMap<Integer, Set<String>> mapA, HashMap<Integer, Set<String>> mapB) {
        HashMap<Integer, Set<String>> result = new HashMap<>();

        // The set returned from keySet is immutable so we have to instantiate new Set in order to use addAll
        Set<Integer> allKeys = new HashSet<>();
        allKeys.addAll(mapA.keySet());
        allKeys.addAll(mapB.keySet());

        for (Integer key : allKeys) {
            Set<String> values = new HashSet<>();

            if (mapA.keySet().contains(key)) {
                values.addAll(mapA.get(key));
            }

            if (mapB.keySet().contains(key)) {
                values.addAll(mapB.get(key));
            }

            result.put(key, values);
        }

        return result;
    }

    private Response constructResponse(boolean validated, String message) throws Exception {
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
