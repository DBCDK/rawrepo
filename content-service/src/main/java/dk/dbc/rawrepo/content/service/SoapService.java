package dk.dbc.rawrepo.content.service;

import com.sun.xml.ws.developer.SchemaValidation;
import dk.dbc.commons.webservice.WsdlValidationErrorHandler;
import javax.annotation.Resource;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.servlet.http.HttpServletRequest;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import org.xml.sax.SAXParseException;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@WebService(serviceName = C.SERVICE, targetNamespace = C.NS, portName = C.PORT)
@SchemaValidation(handler = WsdlValidationErrorHandler.class)
public class SoapService extends Service {

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
