package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@XmlAccessorOrder(value = XmlAccessOrder.UNDEFINED)
public class FetchRequestAuthentication {
    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String user;
    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String group;
    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String password;

    @Override
    public String toString() {
        return "Authentication{" + "user=" + user + ", group=" + group + ", password=???}";
    }

}
