package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponseRecord {

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String bibliographicRecordId;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public Integer agencyId;

    @XmlElements(value = {
        @XmlElement(namespace = C.NS, nillable = false, required = true, name = "content", type = FetchResponseRecordContent.class),
        @XmlElement(namespace = C.NS, nillable = false, required = true, name = "diagnostics", type = String.class)
    })
    public Object content;

    public FetchResponseRecord() {
    }

    public FetchResponseRecord(String bibliographicRecordId, Integer agencyId) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.agencyId = agencyId;
        this.content = new ArrayList<>();
    }

}
