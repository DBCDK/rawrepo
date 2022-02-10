package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS)
@XmlAccessorOrder(value = XmlAccessOrder.UNDEFINED)
public class FetchRequestRecord {

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String bibliographicRecordId;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public Integer agencyId;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public Mode mode;

    @XmlElement(namespace = C.NS, nillable = false, required = false, defaultValue = "false")
    public Boolean allowDeleted;

    @XmlElement(namespace = C.NS, nillable = false, required = false, defaultValue = "false")
    public Boolean includeAgencyPrivate;

    @XmlType(namespace = C.NS, name = "fetchRequestRecordMode")
    public enum Mode {
        RAW,
        MERGED,
        MERGED_DBCKAT,
        COLLECTION
    }

    @Override
    public String toString() {
        return "Record{" + "bibliographicRecordId=" + bibliographicRecordId + ", agencyId=" + agencyId + ", mode=" + mode + ", allowDeleted=" + allowDeleted + ", includeAgencyPrivate=" + includeAgencyPrivate + '}';
    }
}
