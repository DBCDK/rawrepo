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

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class RelationHintsOpenAgency extends RelationHints {
    private static final XLogger logger = XLoggerFactory.getXLogger(RelationHintsOpenAgency.class);
    private final OpenAgencyServiceFromURL openAgencyService;

    public RelationHintsOpenAgency(OpenAgencyServiceFromURL openAgencyService) {
        this.openAgencyService = openAgencyService;
    }

    public RelationHintsOpenAgency(OpenAgencyServiceFromURL openAgencyService, ExecutorService es) {
        super(es);
        this.openAgencyService = openAgencyService;
    }

    @Override
    public boolean usesCommonAgency(int agencyId) throws RawRepoException {
        try {
            return openAgencyService.libraryRules().isAllowed(agencyId, LibraryRuleHandler.Rule.USE_ENRICHMENTS);
        } catch (OpenAgencyException ex) {
            throw new RawRepoException("Cannot access openagency", ex);
        }
    }

    @Override
    public boolean usesCommonSchoolAgency(int agencyId) throws RawRepoException {
        return 300000 < agencyId && agencyId <= 399999;
    }

    @Override
    public List<Integer> provide(Integer agencyId) throws Exception {
        if (usesCommonAgency(agencyId)) {
            return Arrays.asList(870970, 870971, 870979);
        }
        return Arrays.asList(agencyId);
    }

    @Override
    public List<Integer> getProviderOptions(int agencyId) throws RawRepoException{
        List<Integer> agencyPriorityList = new ArrayList<>();
        agencyPriorityList.add(agencyId); // Always use own agency first

        if (usesCommonSchoolAgency(agencyId)) {
            logger.info("School");
            agencyPriorityList.add(300000); // School libraries must use agency 300000 if the local record doesn't exist
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870979);
        } else if (usesCommonAgency(agencyId)) {
            logger.info("FBS");
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870979);
        }

        return agencyPriorityList;
     }

}
