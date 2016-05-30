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

import java.util.Date;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
@XmlType(namespace = C.NS, name = "timestamp")
public class TS {
    @XmlElements(value = {
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "datetime", type = Date.class),
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "millis", type = Long.class)})
    public Object ts;

    public TS() {
        ts = System.currentTimeMillis();
    }

    public TS(long millis) {
        ts = millis;
    }

    public TS(Date date) {
        ts = date;
    }

    public long getMillis() {
        if (ts instanceof Long) {
            return (Long) ts;
        }
        if (ts instanceof Date) {
            return ( (Date) ts ).getTime();
        }
        throw new IllegalStateException("arg");
    }

    public Date getDate() {
        if (ts instanceof Long) {
            return new Date((Long) ts);
        }
        if (ts instanceof Date) {
            return (Date) ts;
        }
        throw new IllegalStateException("arg");
    }

}
