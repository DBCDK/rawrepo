/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.dao.OpenAgencyDAO;
import dk.dbc.rawrepo.dao.RawRepoDAO;
import dk.dbc.rawrepo.json.QueueProvider;
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

            List<String> allowedCatalogingTemplateSets = openAgency.getCatalogingTemplateSets();
            List<String> allowedProviders = rawrepo.getProviders();

            String provider = obj.getString("provider");
            if (provider == null || provider.isEmpty() || !allowedProviders.contains(provider)) {
                LOGGER.error("The provider input {} could not be validated, so returning HTTP 400", provider);
                return returnValidateFailResponse("Provider kunne ikke valideres");
            }
            LOGGER.debug("provider: {}", provider);

            String catalogingTemplateSet = obj.getString("catalogingTemplateSet");
            if (catalogingTemplateSet == null || catalogingTemplateSet.isEmpty() || !allowedCatalogingTemplateSets.contains(catalogingTemplateSet)) {
                LOGGER.error("CatalogingTemplateSet {} could not be validated, so returning HTTP 400", catalogingTemplateSet);
                return returnValidateFailResponse("Provider kunne ikke valideres");
            }
            LOGGER.debug("catalogingTemplateSet: {}", catalogingTemplateSet);

            boolean includeDeleted = obj.getBoolean("includeDeleted");
            LOGGER.debug("includeDeleted: {}", includeDeleted);

            String agencyString = obj.getString("agencyText");
            LOGGER.debug("agencyString: {}", agencyString);
            agencyString = agencyString.replace("\n", ","); // Transform newline and space separation into comma separation
            agencyString = agencyString.replace(" ", ",");
            List<String> agencies = Arrays.asList(agencyString.split(","));
            Set<String> allowedAgencies = openAgency.getLibrariesByCatalogingTemplateSet(catalogingTemplateSet);

            List<String> cleanedAgencyList = new ArrayList<>();
            for (String agency : agencies) {
                String cleanedAgency = agency.trim();
                if (!cleanedAgency.isEmpty()) {
                    cleanedAgencyList.add(cleanedAgency);
                }
            }

            final Pattern p = Pattern.compile("(\\d{6})");

            if (cleanedAgencyList.size() == 0) {
                LOGGER.error("No agencies given, so returning HTTP 400");
                return returnValidateFailResponse("Der skal angives mindst ét biblioteksnummer");
            }

            List<Integer> agencyList = new ArrayList<>();
            for (String agency : cleanedAgencyList) {
                if (!p.matcher(agency).find()) {
                    LOGGER.error("'{}' is not a valid six digit number, so returning HTTP 400", agency);
                    return returnValidateFailResponse("Værdien '" + agency + "' har ikke et gyldigt format for et biblioteksnummer");
                }
                if (!allowedAgencies.containsAll(Collections.singleton(agency))) {
                    LOGGER.error("Agency '{}' is not a valid {} agency, so returning HTTP 400", agency, catalogingTemplateSet);
                    return returnValidateFailResponse("Biblioteksnummeret " + agency + " tilhører ikke biblioteksgruppen " + catalogingTemplateSet);
                }
                LOGGER.debug("Found agency {} in input agency string", agency);
                agencyList.add(Integer.parseInt(agency));
            }

            LOGGER.debug("A total of {} agencies was found in input. Looking for records...", agencyList.size());
            List<RecordId> recordIds = null;
            if (catalogingTemplateSet.equals("ffu")) {
                recordIds = rawrepo.getFFURecords(agencyList, includeDeleted);
            } else if (catalogingTemplateSet.equals("fbs")) {
                recordIds = rawrepo.getFBSRecords(agencyList, includeDeleted);
            }
            LOGGER.debug("Found a total of {} records", recordIds.size());

            List<String> bibliographicRecordIdList = new ArrayList<>();
            List<Integer> agencyListBulk = new ArrayList<>();
            List<String> providerList = new ArrayList<>();
            List<Boolean> changedList = new ArrayList<>();
            List<Boolean> leafList = new ArrayList<>();

            for (RecordId recordId : recordIds) {
                bibliographicRecordIdList.add(recordId.bibliographicRecordId);
                agencyListBulk.add(recordId.agencyId);
                providerList.add(provider);
                changedList.add(true);
                leafList.add(false);
            }

            LOGGER.debug("{} records will be enqueued", recordIds.size());

            List<RawRepoDAO.EnqueueBulkResult> enqueueBulkResults = rawrepo.enqueueBulk(bibliographicRecordIdList, agencyListBulk, providerList, changedList, leafList);

            Integer wasEnqueued = 0;

            for (RawRepoDAO.EnqueueBulkResult bulkResult : enqueueBulkResults) {
                if (bulkResult.queued) {
                    wasEnqueued++;
                }
            }

            JsonObjectBuilder responseJSON = Json.createObjectBuilder();
            responseJSON.add("validated", true);
            responseJSON.add("recordCount", recordIds.size());
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
