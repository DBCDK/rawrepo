/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
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
package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.*;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class RelationHintsOpenAgencyTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(getPort()).withRootDirectory(getPath()));

    static int fallbackPort = (int) ( 15000.0 * Math.random() ) + 15000;

    static int getPort() {
        int port = Integer.parseInt(System.getProperty("wiremock.port", "0"));
        if(port == 0)
            port = fallbackPort;
        return port;
    }
    static String getPath() {
        return wireMockConfig().filesRoot().child("RelationHintsOpenAgency").getPath();
    }

    @Before
    public void setUp() {
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='123456']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_123456.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870970']")
                        .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870970.xml")));
    }

    @Test
    public void testUsesCommonAgency() throws Exception {
        OpenAgencyServiceFromURL service = OpenAgencyServiceFromURL.builder().build("http://localhost:" + getPort() + "/openagency/");

        RelationHintsOpenAgency relationHints = new RelationHintsOpenAgency(service);

        boolean usesCommonAgency;
        usesCommonAgency = relationHints.usesCommonAgency(870970);
        assertEquals(true, usesCommonAgency);

        List<Integer> agencies;
        agencies = relationHints.get(870970);
        assertEquals(Arrays.asList(191919), agencies);

        usesCommonAgency = relationHints.usesCommonAgency(123456);
        assertEquals(false, usesCommonAgency);

        agencies = relationHints.get(123456);
        assertEquals(Arrays.asList(), agencies);

    }

}