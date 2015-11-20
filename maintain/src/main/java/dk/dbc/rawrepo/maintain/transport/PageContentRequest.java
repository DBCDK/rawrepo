package dk.dbc.rawrepo.maintain.transport;

import java.util.ArrayList;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author bogeskov
 */
@XmlRootElement(namespace = C.NS)
@XmlAccessorOrder(XmlAccessOrder.UNDEFINED)
public class PageContentRequest {

    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public String method;

    @XmlElementWrapper(namespace = C.NS, nillable = false, required = false, name = "values")
    @XmlElement(namespace = C.NS, required = true, nillable = false, name = "entry")
    public ArrayList<ValueEntry> values;

    @XmlElement(namespace = C.NS, required = false, nillable = false)
    public String leaving;

    @XmlElement(namespace = C.NS, required = false, nillable = false)
    public String trackingId;


}
