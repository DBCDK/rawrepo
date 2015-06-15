/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-content-service
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service.transport;

import dk.dbc.rawrepo.content.service.C;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
@XmlRootElement(namespace = C.NS)
public class FetchRequest {

    @XmlElement(namespace = C.NS, nillable = false, required = false)
    public FetchRequestAuthentication authentication;

    @XmlElementWrapper(namespace = C.NS, nillable = false, required = true, name = "records")
    @XmlElement(namespace = C.NS, nillable = false, required = true, name = "record")
    public List<FetchRequestRecord> records;
}
