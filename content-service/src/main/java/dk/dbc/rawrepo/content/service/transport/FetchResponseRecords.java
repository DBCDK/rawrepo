package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponseRecords {

    @XmlElement(namespace = C.NS, nillable = false, required = true, name = "record")
    public List<FetchResponseRecord> records;

    public FetchResponseRecords() {
        records = new ArrayList<>();
    }
}
