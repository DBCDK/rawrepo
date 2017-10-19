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
        LOGGER.info(inputStr);
        String res = "";

        try {
            JsonReader reader = Json.createReader(new StringReader(inputStr));
            JsonObject obj = reader.readObject();

            List<QueueType> allowedQueueTypes = openAgency.getQueueTypes();
            List<String> allowedProviders = rawrepo.getProviders();

            String provider = obj.getString("provider");
            if (provider == null || provider.isEmpty() || !allowedProviders.contains(provider)) {
                LOGGER.error("The provider input {} could not be validated, so returning HTTP 400", provider);
                return returnValidateFailResponse("Provider kunne ikke valideres");
            }
            LOGGER.debug("provider: {}", provider);

            QueueType queueType = QueueType.fromString(obj.getString("queueType"));
            if (queueType == null) {
                LOGGER.error("QueueType key {} could not be validated, so returning HTTP 400", obj.getString("queueType"));
                return returnValidateFailResponse("Køtype kunne ikke valideres");
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
                LOGGER.error("No agencies given, so returning HTTP 400");
                return returnValidateFailResponse("Der skal angives mindst ét biblioteksnummer");
            }

            List<Integer> agencyList = new ArrayList<>();
            final Pattern p = Pattern.compile("(\\d{6})");
            for (String agency : cleanedAgencyList) {
                if (!p.matcher(agency).find()) {
                    LOGGER.error("'{}' is not a valid six digit number, so returning HTTP 400", agency);
                    return returnValidateFailResponse("Værdien '" + agency + "' har ikke et gyldigt format for et biblioteksnummer");
                }
                if (!allowedAgencies.containsAll(Collections.singleton(agency))) {
                    LOGGER.error("Agency '{}' is not a valid {} agency, so returning HTTP 400", agency, queueType.getCatalogingTemplateSet());
                    return returnValidateFailResponse("Biblioteksnummeret " + agency + " tilhører ikke biblioteksgruppen " + queueType.getCatalogingTemplateSet());
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
                    changedList.add(true);
                    leafList.add(true);
                }
            }

            LOGGER.debug("{} records will be enqueued", recordMap.size());

            List<RawRepoDAO.EnqueueBulkResult> enqueueBulkResults = rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

            Integer wasEnqueued = 0;

            for (RawRepoDAO.EnqueueBulkResult bulkResult : enqueueBulkResults) {
                if (bulkResult.queued) {
                    wasEnqueued++;
                }
            }

            JsonObjectBuilder responseJSON = Json.createObjectBuilder();
            responseJSON.add("validated", true);
            responseJSON.add("recordCount", recordMap.size());
            responseJSON.add("wasEnqueued", wasEnqueued);
            responseJSON.add("queueTotal", enqueueBulkResults.size());

            res = responseJSON.build().toString();

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            LOGGER.error("Something happened:", e);
            return Response.serverError().build();
        } finally {
            LOGGER.exit(res);
        }
    }

    private Response returnValidateFailResponse(String message) throws Exception {
        JsonObjectBuilder responseJSON = Json.createObjectBuilder();
        responseJSON.add("validated", false);
        responseJSON.add("message", message);

        String res = responseJSON.build().toString();

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
