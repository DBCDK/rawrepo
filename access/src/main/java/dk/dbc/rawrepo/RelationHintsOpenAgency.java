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

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class RelationHintsOpenAgency extends RelationHints {

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
    public List<Integer> provide(Integer agencyId) throws Exception {
        if (usesCommonAgency(agencyId)) {
            return Arrays.asList(870970);
        }
        return Arrays.asList(agencyId);
    }

}
