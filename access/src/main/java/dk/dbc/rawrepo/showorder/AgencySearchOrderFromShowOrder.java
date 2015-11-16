/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
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
import dk.dbc.openagency.client.ShowOrder;
import dk.dbc.rawrepo.AgencySearchOrder;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements {@link AgencySearchOrder}, using openagency webservicecall
 * showOrder
 *
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencySearchOrderFromShowOrder extends AgencySearchOrder {

    private final OpenAgencyServiceFromURL service;
    private final ShowOrder showOrder;

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
        this.showOrder = service.showOrder();
    }

    public AgencySearchOrderFromShowOrder(String url, String user, String group, String password) throws MalformedURLException {
        service = OpenAgencyServiceFromURL.builder()
                .authentication(user, group, password)
                .build(url);
        this.showOrder = service.showOrder();
    }

    public AgencySearchOrderFromShowOrder(OpenAgencyServiceFromURL service) {
        this.service = service;
        this.showOrder = service.showOrder();
    }

    @Override
    public List<Integer> provide(Integer agencyId) throws Exception {
        ArrayList<Integer> agencies = new ArrayList<>();
        boolean seen = false;
            for (String agencyAsString : fetchOrder(agencyId)) {
                int agency = Integer.parseInt(agencyAsString, 10);
                seen = seen || agency == agencyId;
                agencies.add(agency);
            }
            if (!seen) {
                agencies.add(0, agencyId);
            }
            return agencies;
    }

    List<String> fetchOrder(Integer agencyId) throws OpenAgencyException {
        return showOrder.getOrder(agencyId);
    }

}
