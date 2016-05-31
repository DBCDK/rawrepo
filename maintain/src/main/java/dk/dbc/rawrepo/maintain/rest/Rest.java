/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2016 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain.rest;

import dk.dbc.rawrepo.maintain.transport.C;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import javax.annotation.Resource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC <dbc.dk>
 */
@Path("/")
public class Rest {
    private static final Logger log = LoggerFactory.getLogger(Rest.class);

    @Resource(lookup = C.PROPERTIES)
    Properties properties;

    @GET
    @Path("/redirect/{name}")
    public Response redirect(@PathParam("name") String name) {
        log.info("HERE");
        if(properties.containsKey("redirect-" + name)) {
            try {
                return Response.seeOther(new URI(properties.getProperty("redirect-" + name))).build();
            } catch (URISyntaxException ex) {
                log.error("Cound not parse: " + properties.getProperty("redirect-" + name) + " from redirect-" + name);
                return Response.serverError().build();
            }
        } else {
            log.warn("Redirecting nod declared for: redirect-" + name);
            return Response.status(Response.Status.NOT_FOUND).build();
        }

    }
}
