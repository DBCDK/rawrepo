/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.dao.OpenAgencyDAO;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Set;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_LIBRARY)
public class LibraryAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(LibraryAPI.class);

    @EJB
    private OpenAgencyDAO openAgency;

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_LIBRARY_CATALOGING_TEMPLATE_SET + "/{template}")
    public Response getLibrariesByCatalogingTemplateSet(@PathParam("template") String template) {
        LOGGER.entry(template);
        String res = "";
        try {
            try {
                Set<String> libraries = openAgency.getLibrariesByCatalogingTemplateSet(template);


                JsonObjectBuilder outerJSON = Json.createObjectBuilder();
                JsonArrayBuilder innerJSON = Json.createArrayBuilder();
                for (String set : libraries) {
                    innerJSON.add(set);
                }

                outerJSON.add("values", innerJSON);
                JsonObject jsonObject = outerJSON.build();
                res = jsonObject.toString();

                return Response.ok(res, MediaType.APPLICATION_JSON).build();
            } catch (OpenAgencyException e) {
                return Response.serverError().build();
            }
        } finally {
            LOGGER.exit(res);
        }
    }


    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_LIBRARY_CATALOGING_TEMPLATE_SETS)
    public Response getCatalogingTemplateSets() {
        LOGGER.entry();

        List<String> catalogingTemplateSets = openAgency.getCatalogingTemplateSets();

        String res = "";
        try {
            JsonObjectBuilder outerJSON = Json.createObjectBuilder();
            JsonArrayBuilder innerJSON = Json.createArrayBuilder();

            for (String set : catalogingTemplateSets) {
                innerJSON.add(set);
            }
            outerJSON.add("values", innerJSON);
            JsonObject jsonObject = outerJSON.build();
            res = jsonObject.toString();
            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } finally {
            LOGGER.exit(res);
        }
    }


}
