package dk.dbc.commons.webservice;

import javax.ejb.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author bogeskov
 */
@Singleton
public class WebMethodAdaptor {

    private static final Logger log = LoggerFactory.getLogger(WebMethodAdaptor.class);

    private final ValidatorProvider validators;
    private final InvokerProvider invokers;

    public WebMethodAdaptor() {
        this.invokers = new InvokerProvider();
        this.validators = new ValidatorProvider();
    }

    /**
     *
     * @param service   service object, with methods annotated with
     *                  {@link javax.jws.WebMethod @WebMethod}
     * @param operation name of the operation that
     *                  {@link javax.jws.WebMethod @WebMethod} declares
     * @param req       request data object annotated with
     *                  {@link javax.xml.bind.annotation.XmlRootElement @XmlRootElement}
     * @param resp      response data object annotated with
     *                  {@link javax.xml.bind.annotation.XmlRootElement @XmlRootElement}
     * @throws WebMethodCallException
     * @throws WebMethodValidationException
     */
    public void invoke(Object service, String operation, Object req, Object resp) throws WebMethodCallException, WebMethodValidationException {
        try {
            log.trace("invoke()");
            validate(req);
            log.trace("validated req");
            Invoker invoker = invokers.getInvoker(service.getClass(), operation, req.getClass(), resp.getClass());
            log.trace("invoker");
            invoker.invoke(service, req, resp);
            log.trace("invoked");
            validate(resp);
            log.trace("validated resp");
        } catch (WebMethodCallException | WebMethodValidationException ex) {
            throw ex;
        } catch (RuntimeException | ClassNotFoundException ex) {
            throw new WebMethodCallException("Error contructing validator: " + ex.getMessage());
        }
    }

    private void validate(Object obj) throws WebMethodCallException, WebMethodValidationException {
        Validator validator = validators.getValidator(obj.getClass());
        validator.validate("", obj, true, true);
    }
}
