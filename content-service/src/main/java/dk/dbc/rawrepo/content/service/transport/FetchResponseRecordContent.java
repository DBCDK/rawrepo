package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@XmlAccessorOrder(XmlAccessOrder.UNDEFINED)
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponseRecordContent {

    @XmlElement(namespace = C.NS, nillable = false, required = false)
    public String mimeType;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public byte[] data;

    public FetchResponseRecordContent() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public FetchResponseRecordContent(String mimeType, byte[] data) {
        this.mimeType = mimeType;
        this.data = data;
    }
}
