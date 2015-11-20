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
package dk.dbc.rawrepo.maintain.transport;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
@XmlRootElement(namespace = C.NS)
public class StandardResponse {

    @XmlElements({
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "result", type = Result.class),
        @XmlElement(namespace = C.NS, required = true, nillable = false, name = "error", type = ResponseError.class)
    })
    public Object out;

    @XmlElement(namespace = C.NS, required = false, nillable = false)
    public String trackingId;

    @XmlElement(namespace = C.NS, required = false, nillable = false, name = "timestamp")
    public TS timestamp;

    @XmlType(namespace = C.NS, name = "standardResult")
    @XmlAccessorOrder(XmlAccessOrder.UNDEFINED)
    public static class Result {

        @XmlType(namespace = C.NS)
        public static enum Status {

            SUCCESS, PARTIAL, FAILURE
        };

        @XmlElement(namespace = C.NS, required = true, nillable = false)
        public Status status;

        @XmlElement(namespace = C.NS, required = true, nillable = false)
        public String message;

        @XmlElementWrapper(namespace = C.NS, nillable = false, required = false, name = "diags")
        @XmlElement(namespace = C.NS, required = false, nillable = false, name = "diag")
        public ArrayList<Diag> diags;

        @XmlType(namespace = C.NS)
        public static class Diag {

            @XmlElement(namespace = C.NS, required = true, nillable = false)
            public String title;

            @XmlElement(namespace = C.NS, required = true, nillable = false)
            public String message;

            public Diag() {
            }

            @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", justification = "Marshall/Unmarshall")
            public Diag(String title, String message) {
                this.title = title;
                this.message = message;
            }
        }

        public Result() {
        }

        @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", justification = "Marshall/Unmarshall")
        public Result(Status status, String message) {
            this.status = status;
            this.message = message;
            this.diags = new ArrayList<>();
        }

        public Result(Status status, String message, List<Diag> diags) {
            this.status = status;
            this.message = message;
            this.diags = diags instanceof ArrayList ? (ArrayList<Diag>) diags : new ArrayList<>(diags);
        }

    }

}
