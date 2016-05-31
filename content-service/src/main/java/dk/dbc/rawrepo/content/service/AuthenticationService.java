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
import java.util.Properties;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class AuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationService.class);

    @Resource(lookup = C.PROPERTIES_LOOKUP)
    Properties props;

    ForsRightsServiceFromURL service;
    ForsRights forsRights;
    boolean disabled;
    String rightsName;
    String rightsRight;

    @PostConstruct
    public void init() {
        log.debug("init()");
        disabled = Boolean.parseBoolean(props.getProperty(C.FORS.DISABLED, C.FORS.DISABLED_DEFAULT));
        if (disabled) {
            log.info("Validation disabled");
            return;
        }

        String url = props.getProperty(C.FORS.URL, C.FORS.URL_DEFAULT);
        service = ForsRightsServiceFromURL.builder()
                .connectTimeout(getInteger(C.FORS.CONNECT_TIMEOUT, C.FORS.CONNECT_TIMEOUT_DEFAULT))
                .requestTimeout(getInteger(C.FORS.REQUEST_TIMEOUT, C.FORS.REQUEST_TIMEOUT_DEFAULT))
                .build(url);
        log.debug("forsrights url = " + url);

        String cache = props.getProperty(C.FORS.CACHE, C.FORS.CACHE_DEFAULT);
        long cacheAgeSeconds = Long.parseLong(cache);
        ForsRights.RightsCache rightsCache = new ForsRights.RightsCache(cacheAgeSeconds * 1000);
        log.debug("cacheAgeSeconds = " + cacheAgeSeconds);

        rightsName = props.getProperty(C.FORS.RIGHTS_NAME, C.FORS.RIGHTS_NAME_DEFAULT);
        rightsRight = props.getProperty(C.FORS.RIGHTS_RIGHT, C.FORS.RIGHTS_RIGHT_DEFAULT);

        forsRights = service.forsRights(rightsCache);
    }

    private int getInteger(String key, String defaultValue) {
        String value = props.getProperty(key, defaultValue);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            log.warn("NumberFormatException in: " + value + " from " + key);
            return Integer.parseInt(value);
        }
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
