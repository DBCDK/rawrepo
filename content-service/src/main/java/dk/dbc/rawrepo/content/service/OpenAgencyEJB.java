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
package dk.dbc.rawrepo.content.service;

import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class OpenAgencyEJB {
    private static final Logger log = LoggerFactory.getLogger(OpenAgencyEJB.class);

    private OpenAgencyServiceFromURL openAgencyService;

    @PostConstruct
    public void init() {
        log.debug("init()");

        if (!System.getenv().containsKey(C.OPENAGENCY.URL)) {
            throw new RuntimeException("OPENAGENCY_URL must have a value");
        }

        OpenAgencyServiceFromURL.Builder builder = OpenAgencyServiceFromURL.builder();

        builder = builder.
                connectTimeout(Integer.parseInt(System.getenv().getOrDefault(
                        C.OPENAGENCY.CONNECT_TIMEOUT,
                        C.OPENAGENCY.CONNECT_TIMEOUT_DEFAULT))).
                requestTimeout(Integer.parseInt(System.getenv().getOrDefault(
                        C.OPENAGENCY.REQUEST_TIMEOUT,
                        C.OPENAGENCY.REQUEST_TIMEOUT_DEFAULT))).
                setCacheAge(Integer.parseInt(System.getenv().getOrDefault(
                        C.OPENAGENCY.CACHE_AGE,
                        C.OPENAGENCY.CACHE_AGE_DEFAULT)));

        openAgencyService = builder.build(System.getenv().get(C.OPENAGENCY.URL));
    }

    @Lock(LockType.READ)
    public OpenAgencyServiceFromURL getOpenAgencyService() {
        return openAgencyService;
    }

}
