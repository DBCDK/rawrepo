/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-content-service
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import com.sun.xml.ws.developer.SchemaValidation;
import dk.dbc.commons.webservice.WsdlValidationErrorHandler;
import javax.annotation.Resource;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.servlet.http.HttpServletRequest;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
@WebService(serviceName = C.SERVICE, targetNamespace = C.NS, portName = C.PORT)
@SchemaValidation(handler = WsdlValidationErrorHandler.class)
public class SoapService extends Service {
    private static final Logger log = LoggerFactory.getLogger(SoapService.class);

    @Resource
    private WebServiceContext webServiceContext;


    @WebMethod(exclude = true)
    @Override
    public SAXParseException getException() {
        return WsdlValidationErrorHandler.fetchSAXParseException(webServiceContext);
    }

    @WebMethod(exclude = true)
    @Override
    public String getIp() {
        MessageContext messageContext = webServiceContext.getMessageContext();
        HttpServletRequest httpServletRequest = (HttpServletRequest) messageContext.get(MessageContext.SERVLET_REQUEST);
        return httpServletRequest.getRemoteAddr();
    }

    @WebMethod(exclude = true)
    @Override
    public String getXForwardedFor() {
        MessageContext messageContext = webServiceContext.getMessageContext();
        HttpServletRequest httpServletRequest = (HttpServletRequest) messageContext.get(MessageContext.SERVLET_REQUEST);
        return httpServletRequest.getHeader("x-forwarded-for");
    }
}
