package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlRootElement(namespace = C.NS)
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponse {

    @XmlElements({
        @XmlElement(namespace = C.NS, name = "records", nillable = false, required = true, type = FetchResponseRecords.class),
        @XmlElement(namespace = C.NS, name = "error", nillable = false, required = true, type = FetchResponseError.class)
    })
    public Object out;
}
