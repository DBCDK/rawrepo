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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class FetchResponseError {

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public String message;

    @XmlElement(namespace = C.NS, nillable = false, required = true)
    public Type type;

    @XmlType(namespace = C.NS, name = "fetchResponseErrorType")
    public static enum Type {

        UNKNOWN, REQUEST_CONTENT_ERROR, AUTHENTICATION_DENIED, INTERNAL_SERVER_ERROR
    }

    public FetchResponseError() {
        this.message = "Unknown";
        this.type = Type.UNKNOWN;
    }

    public FetchResponseError(String message, Type type) {
        this.message = message;
        this.type = type;
    }

}
