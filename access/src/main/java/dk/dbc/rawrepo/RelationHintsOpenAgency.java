/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author DBC {@literal <dbc.dk>}
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
    public List<Integer> getProviderOptions(int agencyId) throws RawRepoException {
        List<Integer> agencyPriorityList = new ArrayList<>();
        agencyPriorityList.add(agencyId); // Always use own agency first

        if (usesCommonSchoolAgency(agencyId)) {
            agencyPriorityList.add(300000); // School libraries must use agency 300000 if the local record doesn't exist
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870979);
        } else if (usesCommonAgency(agencyId)) {
            agencyPriorityList.add(870970);
            agencyPriorityList.add(870979);
        }

        return agencyPriorityList;
    }

}
