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

import dk.dbc.forsrights.client.ForsRights;
import dk.dbc.forsrights.client.ForsRightsException;
import dk.dbc.forsrights.client.ForsRightsServiceFromURL;
import dk.dbc.rawrepo.content.service.transport.FetchRequestAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class AuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationService.class);

    private boolean disabled;
    private String rightsName;
    private String rightsRight;

    private ForsRights forsRights;

    @PostConstruct
    public void init() {
        log.debug("init()");

        disabled = Boolean.parseBoolean(System.getenv().getOrDefault(
                C.FORS.DISABLED,
                C.FORS.DISABLED_DEFAULT));

        if (disabled) {
            log.info("Validation disabled");
            return;
        }

        if (!System.getenv().containsKey(C.FORS.URL)) {
            throw new RuntimeException("FORSRIGHTS_URL must have a value");
        }

        rightsName = System.getenv().getOrDefault(
                C.FORS.RIGHTS_NAME,
                C.FORS.RIGHTS_NAME_DEFAULT);

        rightsRight = System.getenv().getOrDefault(
                C.FORS.RIGHTS_RIGHT,
                C.FORS.RIGHTS_RIGHT_DEFAULT);

        ForsRightsServiceFromURL service = ForsRightsServiceFromURL.builder()
                .connectTimeout(Integer.parseInt(System.getenv().getOrDefault(
                        C.FORS.CONNECT_TIMEOUT,
                        C.FORS.CONNECT_TIMEOUT_DEFAULT)))
                .requestTimeout(Integer.parseInt(System.getenv().getOrDefault(
                        C.FORS.REQUEST_TIMEOUT,
                        C.FORS.REQUEST_TIMEOUT_DEFAULT)))
                .build(System.getenv(C.FORS.URL));

        long cacheAgeSeconds = Integer.parseInt(System.getenv().getOrDefault(
                C.FORS.CACHE,
                C.FORS.CACHE_DEFAULT));

        ForsRights.RightsCache rightsCache = new ForsRights.RightsCache(cacheAgeSeconds * 1000);
        log.debug("cacheAgeSeconds = {}", cacheAgeSeconds);

        forsRights = service.forsRights(rightsCache);
    }


    public boolean validate(String ip) throws ForsRightsException {
        if (disabled) {
            return true;
        }
        log.debug("validate(ip)");
        ForsRights.RightSet rights = forsRights.lookupRight("", "", "", ip);
        return rights.hasRight("", "");
    }

    public boolean validate(FetchRequestAuthentication authentication) throws ForsRightsException {
        if (disabled) {
            return true;
        }
        log.debug("validate(user/group/password)");
        ForsRights.RightSet rights = forsRights.lookupRight(authentication.user, authentication.group, authentication.password, "");
        return rights.hasRight(rightsName, rightsRight);
    }

}
