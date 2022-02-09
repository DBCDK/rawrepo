package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@XmlAccessorOrder(value = XmlAccessOrder.UNDEFINED)
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponseError {

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String message;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public Type type;

    @XmlType(namespace = C.NS, name = "fetchResponseErrorType")
    public enum Type {
        UNKNOWN, REQUEST_CONTENT_ERROR, AUTHENTICATION_DENIED, INTERNAL_SERVER_ERROR
    }

    public FetchResponseError() {
        this.message = "Unknown";
        this.type = Type.UNKNOWN;
    }

    public FetchResponseError(String message, Type type) {
        this.message = message;
        this.type = type;
    }

}
