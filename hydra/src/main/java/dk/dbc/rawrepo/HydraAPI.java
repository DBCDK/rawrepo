package dk.dbc.rawrepo;

import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path(ApplicationConstants.API_HYDRA)
public class HydraAPI {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(HydraAPI.class);

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_HYDRA_STATUS)
    public Response getStatus() {
        return Response.ok(MediaType.APPLICATION_JSON).build();
    }

    @Stopwatch
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path(ApplicationConstants.API_HYDRA_INSTANCE_NAME)
    public Response getInstanceName() {
        String res = "";
        try {
            JsonObjectBuilder jsonObject = Json.createObjectBuilder();
            jsonObject.add("value", System.getenv().getOrDefault(ApplicationConstants.INSTANCE_NAME, "INSTANCE_NAME not set"));

            res = jsonObject.build().toString();

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } finally {
            LOGGER.exit(res);
        }
    }
}
