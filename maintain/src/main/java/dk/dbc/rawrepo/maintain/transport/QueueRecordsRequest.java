package dk.dbc.rawrepo.maintain.transport;

/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-maintain
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
@XmlRootElement(namespace = C.NS)
@XmlAccessorOrder(XmlAccessOrder.UNDEFINED)
public class QueueRecordsRequest {

    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public Integer agencyId;

    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public RecordIds ids;

    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public String provider;

    @XmlElement(namespace = C.NS, required = false, nillable = false)
    public String trackingId;
}
