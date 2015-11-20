package dk.dbc.rawrepo.maintain.transport;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author bogeskov
 */
@XmlRootElement(namespace = C.NS)
public class PageContentResponse {

    @XmlElements({
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "result", type = Result.class),
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "error", type = ResponseError.class)
    })
    public Object out;

    @XmlElement(namespace = C.NS, required = false, nillable = false)
    public String trackingId;

    /**
     *
     */
    @XmlType(namespace = C.NS)
    public static class Result {

        @XmlElementWrapper(namespace = C.NS, nillable = false, required = false, name = "values")
        @XmlElement(namespace = C.NS, required = false, nillable = false, name = "entry")
        public ArrayList<ValueEntry> values;

        public Result() {
        }

        @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", justification = "Marshall/Unmarshall")
        public Result(ArrayList<ValueEntry> values) {
            this.values = values;
        }

    }
}
