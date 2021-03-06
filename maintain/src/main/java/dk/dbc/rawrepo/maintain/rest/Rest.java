/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2016 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
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
package dk.dbc.rawrepo.maintain.rest;

import dk.dbc.rawrepo.maintain.QueueRules;
import dk.dbc.rawrepo.maintain.QueueRules.Provider;
import dk.dbc.rawrepo.maintain.QueueRules.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@Path("/")
public class Rest {
    private static final Logger log = LoggerFactory.getLogger(Rest.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @GET
    @Path("queuerules")
    @Produces("text/html")
    public String getQueuerules() throws SQLException {
        try (QueueRules q = new QueueRules(dataSource)) {
            ArrayList<Provider> providers = q.getQueueRules();
            log.debug("Found '{}' providers", providers.size());
            StringBuilder sb = new StringBuilder();
            sb.append("<!DOCTYPE html>");
            sb.append("<html>");
            sb.append("<head>");
            sb.append("<meta charset=\"UTF-8\">");
            sb.append("<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/queuerules.css\">");
            sb.append("</head>");
            sb.append("<body>");
            sb.append("<table>");

            sb.append("<tr><th>Provider</th><th>Worker</th><th>Description</th></tr>");

            for (Provider provider : providers) {
                for (int i = 0; i < provider.getWorkers().size(); i++) {

                    boolean first = i == 0;
                    boolean last = i == provider.getWorkers().size() - 1;

                    Worker w = provider.getWorkers().get(i);

                    if (last) {
                        sb.append("<tr class=\"last\">");
                    } else {
                        sb.append("<tr>");
                    }

                    if (first) {
                        sb.append("<td rowspan=\"").append(provider.getWorkers().size()).append("\">").append(provider.getProvider()).append("</td>");
                    }
                    sb.append("<td>").append(w.getWorker()).append("</td>");
                    sb.append("</tr>");
                }
            }

            sb.append("</table>");
            sb.append("</body>");
            sb.append("</html>");

            return sb.toString();
        }

    }


}
