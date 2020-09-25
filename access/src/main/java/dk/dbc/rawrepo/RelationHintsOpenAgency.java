/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RelationHintsOpenAgency {
    private final OpenAgencyServiceFromURL openAgencyService;

    public RelationHintsOpenAgency(OpenAgencyServiceFromURL openAgencyService) {
        this.openAgencyService = openAgencyService;
    }

    public boolean usesCommonAgency(int agencyId) throws RawRepoException {
        try {
            return openAgencyService.libraryRules().isAllowed(agencyId, LibraryRuleHandler.Rule.USE_ENRICHMENTS);
        } catch (OpenAgencyException ex) {
            throw new RawRepoException("Cannot access openagency", ex);
        }
    }

    private boolean usesAuthorityAgency(int agencyId) {
        return 870970 == agencyId || 870971 == agencyId || 870974 == agencyId;
    }

    public boolean usesCommonSchoolAgency(int agencyId) {
        return 300000 < agencyId && agencyId <= 399999;
    }

    public List<Integer> get(Integer agencyId) throws RawRepoException {
        if (usesCommonAgency(agencyId)) {
            return Arrays.asList(870970, 870971, 870974, 870976, 870979, 190002, 190004);
        }
        return Arrays.asList(agencyId);
    }

    public List<Integer> getAgencyPriority(int agencyId) throws RawRepoException {
        List<Integer> agencyPriorityList = new ArrayList<>();
        agencyPriorityList.add(agencyId); // Always use own agency first

        if (usesCommonSchoolAgency(agencyId)) {
            agencyPriorityList.add(300000); // School libraries must use agency 300000 if the local record doesn't exist
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870979);
        } else if (usesCommonAgency(agencyId)) {
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870971);
            agencyPriorityList.add(870974);
            agencyPriorityList.add(870976);
            agencyPriorityList.add(870979);
            agencyPriorityList.add(190002);
            agencyPriorityList.add(190004);
        } else if (usesAuthorityAgency(agencyId)) {
            agencyPriorityList.add(870979);
        }

        return agencyPriorityList;
    }

}
