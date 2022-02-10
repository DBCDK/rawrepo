package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@XmlRootElement(namespace = C.NS)
public class FetchRequest {

    @XmlElement(namespace = C.NS, nillable = false, required = false)
    public FetchRequestAuthentication authentication;

    @XmlElementWrapper(namespace = C.NS, nillable = false, required = true, name = "records")
    @XmlElement(namespace = C.NS, nillable = false, required = true, name = "record")
    public List<FetchRequestRecord> records;
}
