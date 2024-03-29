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

import com.github.tomakehurst.wiremock.WireMockServer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.vipcore.VipCoreConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.client.Client;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RelationHintsVipCoreTest {

    private static WireMockServer wireMockServer;
    private static String wireMockHost;

    final static Client CLIENT = HttpClient.newClient(new ClientConfig()
            .register(new JacksonFeature()));
    private static VipCoreLibraryRulesConnector connector;

    @BeforeAll
    static void startWireMockServer() {
        wireMockServer = new WireMockServer(options().dynamicPort()
                .dynamicHttpsPort()
                .withRootDirectory(wireMockConfig().filesRoot().child("wiremock/vipcore").getPath()));
        wireMockServer.start();
        wireMockHost = "http://localhost:" + wireMockServer.port();
        configureFor("localhost", wireMockServer.port());
    }

    @BeforeAll
    static void setConnector() {
        connector = new VipCoreLibraryRulesConnector(CLIENT, wireMockHost, 0, VipCoreConnector.TimingLogLevel.INFO);
    }

    @AfterAll
    static void stopWireMockServer() {
        wireMockServer.stop();
    }

    @Test
    public void testUsesCommonAgency() throws Exception {
        final RelationHintsVipCore relationHints = new RelationHintsVipCore(connector);

        relationHints.usesCommonSchoolAgency(191919);

        boolean usesCommonAgency;
        usesCommonAgency = relationHints.usesCommonAgency(191919);
        assertTrue(usesCommonAgency);

        List<Integer> agencies;
        agencies = relationHints.get(191919);
        assertThat(Arrays.asList(870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        usesCommonAgency = relationHints.usesCommonAgency(123456);
        assertFalse(usesCommonAgency);

        agencies = relationHints.get(123456);
        assertThat(Collections.singletonList(123456), is(agencies));

        agencies = relationHints.getAgencyPriority(123456);
        assertThat(Collections.singletonList(123456), is(agencies));

        agencies = relationHints.getAgencyPriority(861620);
        assertThat(Arrays.asList(861620, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870970);
        assertThat(Arrays.asList(870970, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870971);
        assertThat(Arrays.asList(870971, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870974);
        assertThat(Arrays.asList(870974, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870975);
        assertThat(Arrays.asList(870975, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870976);
        assertThat(Arrays.asList(870976, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870978);
        assertThat(Arrays.asList(870978, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(870979);
        assertThat(Arrays.asList(870979, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(190002);
        assertThat(Arrays.asList(190002, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(190004);
        assertThat(Arrays.asList(190004, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(190007);
        assertThat(Arrays.asList(190007, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));

        agencies = relationHints.getAgencyPriority(190008);
        assertThat(Arrays.asList(190008, 870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008), is(agencies));
    }

}
