/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
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
    public static enum Mode {

        RAW,
        MERGED,
        COLLECTION
    }

    @Override
    public String toString() {
        return "Record{" + "bibliographicRecordId=" + bibliographicRecordId + ", agencyId=" + agencyId + ", mode=" + mode + ", allowDeleted=" + allowDeleted + ", includeAgencyPrivate=" + includeAgencyPrivate + '}';
    }
}
