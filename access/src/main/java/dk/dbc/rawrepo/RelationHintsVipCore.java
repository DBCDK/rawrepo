package dk.dbc.rawrepo;

import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import dk.dbc.vipcore.marshallers.ErrorType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RelationHintsVipCore {
    private final VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;

    public RelationHintsVipCore(VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) {
        this.vipCoreLibraryRulesConnector = vipCoreLibraryRulesConnector;
    }

    public boolean usesCommonAgency(int agencyId) throws RawRepoException, VipCoreException {
        try {
            return vipCoreLibraryRulesConnector.hasFeature(agencyId, VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS);
        } catch (VipCoreException ex) {
            if (ex.getMessage().equals(ErrorType.SERVICE_UNAVAILABLE.value())) {
                throw new RawRepoException("Cannot access vipcore", ex);
            } else {
                throw ex;
            }
        }
    }

    private boolean usesAuthorityAgency(int agencyId) {
        return 870970 == agencyId || 870971 == agencyId || 870974 == agencyId;
    }

    public boolean usesCommonSchoolAgency(int agencyId) {
        return 300000 < agencyId && agencyId <= 399999;
    }

    public List<Integer> get(Integer agencyId) throws RawRepoException, VipCoreException {
        if (usesCommonAgency(agencyId)) {
            return Arrays.asList(870970, 870971, 870974, 870975, 870976, 870978, 870979, 190002, 190004, 190007, 190008);
        }
        return Collections.singletonList(agencyId);
    }

    public List<Integer> getAgencyPriority(int agencyId) throws RawRepoException, VipCoreException {
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
            agencyPriorityList.add(870975);
            agencyPriorityList.add(870976);
            agencyPriorityList.add(870978);
            agencyPriorityList.add(870979);
            agencyPriorityList.add(190002);
            agencyPriorityList.add(190004);
            agencyPriorityList.add(190007);
            agencyPriorityList.add(190008);
        } else if (usesAuthorityAgency(agencyId)) {
            agencyPriorityList.add(870979);
        }

        return agencyPriorityList;
    }

}
