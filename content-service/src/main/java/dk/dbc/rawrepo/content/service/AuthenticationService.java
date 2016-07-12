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

import dk.dbc.eeconfig.EEConfig;
import dk.dbc.forsrights.client.ForsRights;
import dk.dbc.forsrights.client.ForsRightsException;
import dk.dbc.forsrights.client.ForsRightsServiceFromURL;
import dk.dbc.rawrepo.content.service.transport.FetchRequestAuthentication;
import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.inject.Inject;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class AuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationService.class);

    @Inject
    @EEConfig.Name(C.FORS.DISABLED)
    @EEConfig.Default(C.FORS.DISABLED_DEFAULT)
    boolean disabled;

    @Inject
    @EEConfig.Name(C.FORS.URL)
    @EEConfig.Default(C.FORS.URL_DEFAULT)
    @EEConfig.Url
    String forsRightsUrl;

    @Inject
    @EEConfig.Name(C.FORS.CONNECT_TIMEOUT)
    @EEConfig.Default(C.FORS.CONNECT_TIMEOUT_DEFAULT)
    @Min(1)
    int connectTimeout;

    @Inject
    @EEConfig.Name(C.FORS.REQUEST_TIMEOUT)
    @EEConfig.Default(C.FORS.REQUEST_TIMEOUT_DEFAULT)
    @Min(1)
    int requestTimeout;

    @Inject
    @EEConfig.Name(C.FORS.CACHE)
    @EEConfig.Default(C.FORS.CACHE_DEFAULT)
    @Min(1)
    long cacheAgeSeconds;

    @Inject
    @EEConfig.Name(C.FORS.RIGHTS_NAME)
    @EEConfig.Default(C.FORS.RIGHTS_NAME_DEFAULT)
    String rightsName;

    @Inject
    @EEConfig.Name(C.FORS.RIGHTS_RIGHT)
    @EEConfig.Default(C.FORS.RIGHTS_RIGHT_DEFAULT)
    String rightsRight;


    ForsRightsServiceFromURL service;
    ForsRights forsRights;

    @PostConstruct
    public void init() {
        log.debug("init()");
        if (disabled) {
            log.info("Validation disabled");
            return;
        }

        service = ForsRightsServiceFromURL.builder()
        .connectTimeout(connectTimeout)
        .requestTimeout(requestTimeout)
        .build(forsRightsUrl);
        log.debug("forsrights url = " + forsRightsUrl);

        ForsRights.RightsCache rightsCache = new ForsRights.RightsCache(cacheAgeSeconds * 1000);
        log.debug("cacheAgeSeconds = " + cacheAgeSeconds);

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
        System.out.println("rights = " + rights);
        return rights.hasRight(rightsName, rightsRight);
    }

}
