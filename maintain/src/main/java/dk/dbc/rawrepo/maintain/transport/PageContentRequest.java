/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain.transport;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

/**
 * @author DBC {@literal <dbc.dk>}
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
