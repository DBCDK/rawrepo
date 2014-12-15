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

import dk.dbc.rawrepo.AgencySearchOrder;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

/**
 * Implements {@link AgencySearchOrder}, using openagency webservicecall
 * showOrder
 *
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencySearchOrderFromShowOrder extends AgencySearchOrder {

    private final OpenAgencyService service;

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
     * @throws MalformedURLException if url isn't valid
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public AgencySearchOrderFromShowOrder(String url) throws MalformedURLException {
        new URL(url); // validate url syntax
        URL resource = getClass().getResource("/openagency.wsdl");
        Service create = OpenAgencyService.create(resource, new QName("OpenAgencyService"));
        this.service = create.getPort(OpenAgencyService.class);
        this.service.addPort(null, SOAPBinding.SOAP12HTTP_BINDING, url);
    }

    @Override
    public List<Integer> provideAgenciesFor(int agencyId) {
        ArrayList<Integer> agencies = new ArrayList<>();
        boolean seen = false;
        for (String agencyAsString : fetchAgencies(agencyId)) {
            int agency = Integer.parseInt(agencyAsString, 10);
            seen = seen || agency == agencyId;
            agencies.add(agency);
        }
        if (!seen) {
            agencies.add(0, agencyId);
        }
        return agencies;
    }

    List<String> fetchAgencies(int agencyId) throws RuntimeException {
        OpenAgencyPortType portType = service.getOpenAgencyPortType();
        ShowOrderRequest showOrderRequest = new ShowOrderRequest();
        showOrderRequest.setAgencyId(String.valueOf(agencyId));
        ShowOrderResponse showOrder = portType.showOrder(showOrderRequest);
        ErrorType error = showOrder.getError();
        if (error != null) {
            throw new RuntimeException(error.toString());
        }
        return showOrder.getAgencyId();
    }

}
