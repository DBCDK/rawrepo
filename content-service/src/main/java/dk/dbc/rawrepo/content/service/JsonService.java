/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import dk.dbc.commons.webservice.WebMethodAdaptor;
import dk.dbc.commons.webservice.WebMethodCallException;
import dk.dbc.commons.webservice.WebMethodValidationException;
import dk.dbc.rawrepo.content.service.transport.FetchRequest;
import dk.dbc.rawrepo.content.service.transport.FetchResponse;
import dk.dbc.rawrepo.content.service.transport.FetchResponseError;
import javax.ejb.EJB;
import javax.ejb.EJBException;
import javax.ejb.Stateless;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
@Path("")
@Stateless
public class JsonService extends Service {

    private static final Logger log = LoggerFactory.getLogger(JsonService.class);

    @EJB
    private WebMethodAdaptor webMethodAdaptor;

    @Context
    private HttpServletRequest httpServletRequest;

    @POST
    @Path("/" + C.OPERATION_FETCH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    public FetchResponse fetch(FetchRequest req) {
        FetchResponse resp = new FetchResponse();
        try {
            webMethodAdaptor.invoke(this, C.OPERATION_FETCH, req, resp);
        } catch (WebMethodCallException ex) {
            log.error("Caught WebMethodCallException: " + ex.getMessage());
            resp.out = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (WebMethodValidationException ex) {
            log.error("Caught WebMethodValidationException: " + ex.getMessage());
            resp.out = new FetchResponseError(ex.getMessage(), FetchResponseError.Type.REQUEST_CONTENT_ERROR);
        } catch (EJBException ex) {
            log.error("Caught Exception: " + ex.getClass().getSimpleName() + " > " + ex.getMessage());
            resp.out = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (RuntimeException ex) {
            log.error("Caught Exception: " + ex.getClass().getSimpleName() + " > " + ex.getMessage());
            resp.out = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        }
        return resp;
    }

    @Override
    public SAXParseException getException() {
        return null;
    }

    @Override
    public String getIp() {
        return httpServletRequest.getRemoteAddr();
    }

    @Override
    public String getXForwardedFor() {
        String header = httpServletRequest.getHeader("x-forwarded-for");
        return header;
    }
}
