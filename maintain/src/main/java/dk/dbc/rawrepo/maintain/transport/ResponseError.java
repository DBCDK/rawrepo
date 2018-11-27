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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@XmlType(namespace = C.NS, name = "responseError")
@XmlAccessorOrder(value = XmlAccessOrder.UNDEFINED)
public class ResponseError {

    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public String message;
    @XmlElement(namespace = C.NS, required = true, nillable = false)
    public Type type;

    @XmlType(namespace = C.NS, name = "responseErrorType")
    public enum Type {

        UNKNOWN, INTERNAL_SERVER_ERROR, REQUEST_CONTENT_ERROR
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", justification = "Marshall/Unmarshall")
    public ResponseError() {
        this.message = "Unknown";
        this.type = Type.UNKNOWN;
    }

    public ResponseError(String message, Type type) {
        this.message = message;
        this.type = type;
    }

}
