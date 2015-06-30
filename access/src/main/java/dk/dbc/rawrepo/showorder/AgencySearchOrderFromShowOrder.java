/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.showorder;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.AgencySearchOrder;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@link AgencySearchOrder}, using openagency webservicecall
 * showOrder
 *
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencySearchOrderFromShowOrder extends AgencySearchOrder {

    private static final Logger log = LoggerFactory.getLogger(AgencySearchOrderFromShowOrder.class);

    private final OpenAgencyServiceFromURL service;

    /**
     *
     *
     *
     * You might want to set:
     *
     * System.setProperty("http.keepAlive", "true");
     *
     * @param url URL of openagencyservice (at least version 2.17)
     *            http://openagency.addi.dk/2.17/
     */
    public AgencySearchOrderFromShowOrder(String url) {
        service = OpenAgencyServiceFromURL.builder()
                .build(url);
    }

    public AgencySearchOrderFromShowOrder(String url, String user, String group, String password) throws MalformedURLException {
        service = OpenAgencyServiceFromURL.builder()
                .authentication(user, group, password)
                .build(url);
    }

    public AgencySearchOrderFromShowOrder(OpenAgencyServiceFromURL service) {
        this.service = service;
    }

    @Override
    public List<Integer> provideAgenciesFor(int agencyId) {
        ArrayList<Integer> agencies = new ArrayList<>();
        boolean seen = false;
        try {
            for (String agencyAsString : fetchAgencies(agencyId)) {
                int agency = Integer.parseInt(agencyAsString, 10);
                seen = seen || agency == agencyId;
                agencies.add(agency);
            }
            if (!seen) {
                agencies.add(0, agencyId);
            }
            return agencies;
        } catch (OpenAgencyException ex) {
            log.error("Caught: OpenAgencyException: " + ex.getMessage());
            throw new RuntimeException(ex);
        }
    }

    List<String> fetchAgencies(int agencyId) throws OpenAgencyException {
        return service.showOrder().getOrder(agencyId);
    }

}
