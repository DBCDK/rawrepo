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
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import javax.annotation.PostConstruct;
import javax.ejb.Lock;
import javax.ejb.LockType;
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
public class AgencySearchOrderEJB {

    private static final Logger log = LoggerFactory.getLogger(AgencySearchOrderEJB.class);


    @Inject
    @EEConfig.Name(C.OPENAGENCY.URL)
    @EEConfig.Default(C.OPENAGENCY.URL_DEFAULT)
    @EEConfig.Url
    String openAgencyUrl;

    @Inject
    @EEConfig.Name(C.OPENAGENCY.CONNECT_TIMEOUT)
    @EEConfig.Default(C.OPENAGENCY.CONNECT_TIMEOUT_DEFAULT)
    @Min(1)
    int connectTimeout;

    @Inject
    @EEConfig.Name(C.OPENAGENCY.REQUEST_TIMEOUT)
    @EEConfig.Default(C.OPENAGENCY.REQUEST_TIMEOUT_DEFAULT)
    @Min(1)
    int requestTimeout;



    private OpenAgencyServiceFromURL openAgencyService;

    @PostConstruct
    public void init() {
        log.debug("init()");
        openAgencyService = OpenAgencyServiceFromURL.builder()
        .connectTimeout(connectTimeout)
        .requestTimeout(requestTimeout)
        .build(openAgencyUrl);
    }

    @Lock( LockType.READ )
    public OpenAgencyServiceFromURL getOpenAgencyService() {
        return openAgencyService;
    }

}
