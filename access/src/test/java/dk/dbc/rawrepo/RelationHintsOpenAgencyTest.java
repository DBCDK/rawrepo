/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingXPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RelationHintsOpenAgencyTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(getPort()).withRootDirectory(getPath()));

    static int fallbackPort = (int) (15000.0 * Math.random()) + 15000;

    static int getPort() {
        int port = Integer.parseInt(System.getProperty("wiremock.port", "0"));
        if (port == 0)
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
                        matchingXPath("//ns1:agencyId[.='861620']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_861620.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='191919']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_191919.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870970']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870970.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870971']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870971.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870975']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870975.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870974']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870974.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870976']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870976.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='870979']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_870979.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='190002']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_190002.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='190004']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_190004.xml")));
        stubFor(post(urlMatching("/openagency/"))
                .withRequestBody(
                        matchingXPath("//ns1:agencyId[.='190007']")
                                .withXPathNamespace("ns1", "http://oss.dbc.dk/ns/openagency"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBodyFile("openagency_libraryRules_190007.xml")));
    }

    @Test
    public void testUsesCommonAgency() throws Exception {
        OpenAgencyServiceFromURL service = OpenAgencyServiceFromURL.builder().build("http://localhost:" + getPort() + "/openagency/");

        RelationHintsOpenAgency relationHints = new RelationHintsOpenAgency(service);

        boolean usesCommonAgency;
        usesCommonAgency = relationHints.usesCommonAgency(191919);
        assertTrue(usesCommonAgency);

        List<Integer> agencies;
        agencies = relationHints.get(191919);
        assertEquals(Arrays.asList(870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        usesCommonAgency = relationHints.usesCommonAgency(123456);
        assertFalse(usesCommonAgency);

        agencies = relationHints.get(123456);
        assertEquals(Collections.singletonList(123456), agencies);

        agencies = relationHints.getAgencyPriority(123456);
        assertEquals(Collections.singletonList(123456), agencies);

        agencies = relationHints.getAgencyPriority(861620);
        assertEquals(Arrays.asList(861620, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870970);
        assertEquals(Arrays.asList(870970, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870971);
        assertEquals(Arrays.asList(870971, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870974);
        assertEquals(Arrays.asList(870974, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870975);
        assertEquals(Arrays.asList(870975, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870976);
        assertEquals(Arrays.asList(870976, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(870979);
        assertEquals(Arrays.asList(870979, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(190002);
        assertEquals(Arrays.asList(190002, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(190004);
        assertEquals(Arrays.asList(190004, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);

        agencies = relationHints.getAgencyPriority(190007);
        assertEquals(Arrays.asList(190007, 870970, 870971, 870974, 870975, 870976, 870979, 190002, 190004, 190007), agencies);
    }

}
