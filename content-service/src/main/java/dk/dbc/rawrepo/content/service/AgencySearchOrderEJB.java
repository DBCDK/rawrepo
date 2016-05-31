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
import java.util.Properties;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Stateless
public class AgencySearchOrderEJB {

    private static final Logger log = LoggerFactory.getLogger(AgencySearchOrderEJB.class);

    @Resource(lookup = C.PROPERTIES_LOOKUP)
    Properties props;

    private OpenAgencyServiceFromURL openAgencyService;

    @PostConstruct
    public void init() {
        log.debug("init()");
        String url = props.getProperty(C.SEARCHORDER.URL, C.SEARCHORDER.URL_DEFAULT);
        openAgencyService = OpenAgencyServiceFromURL.builder()
        .connectTimeout(getInteger(C.SEARCHORDER.CONNECT_TIMEOUT, C.SEARCHORDER.CONNECT_TIMEOUT_DEFAULT))
        .requestTimeout(getInteger(C.SEARCHORDER.REQUEST_TIMEOUT, C.SEARCHORDER.REQUEST_TIMEOUT_DEFAULT))
        .build(url);
    }

    public OpenAgencyServiceFromURL getOpenAgencyService() {
        return openAgencyService;
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

}
