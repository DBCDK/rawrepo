/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class AgencySearchOrderFallback extends AgencySearchOrder {

    private final List<Integer> searchOrder;
    private final Set<Integer> agenciesInSearchOrder;

    public AgencySearchOrderFallback(String agencies) throws NumberFormatException {
        super(null);
        this.searchOrder = new ArrayList<>();
        for (String agency : agencies.split("[^0-9]+")) {
            this.searchOrder.add(Integer.parseInt(agency));
        }
        this.agenciesInSearchOrder = new HashSet<>(searchOrder);
    }

    public AgencySearchOrderFallback() {
        super(null);
        this.searchOrder = initSearchOrderList();
        this.agenciesInSearchOrder = new HashSet<>(searchOrder);
    }

    @Override
    public List<Integer> provide(Integer agencyId) {
        ArrayList<Integer> list = new ArrayList<>();
        if (!agenciesInSearchOrder.contains(agencyId)) {
            list.add(agencyId);
        }
        list.addAll(searchOrder);
        return list;
    }

    private static List<Integer> initSearchOrderList() {
        return Arrays.asList(191919,
                             870970, 150000, 810015, 810010, 820010, 125010, 810013, 820020, 810011, 810012, 820051, 820050, 820040, 820030, 820110, 850970,
                             125090, 820070, 820140, 820120, 820060, 125020, 125800, 125030, 125080, 125040, 125060, 125070, 125460, 125470, 125490, 125491,
                             820090, 820080, 820150, 830010, 830030, 830040, 830050, 830060, 830080, 830120, 830460, 830130, 830140, 830170, 875180, 830240,
                             830250, 830260, 830270, 830280, 830660, 830300, 830310, 830550, 830800, 830370, 830360, 830380, 830390, 830400, 830410, 830700,
                             830471, 830961, 830470, 830520, 830540, 830500, 830560, 830570, 830580, 830600, 830610, 830690, 830710, 830720, 830740, 830750,
                             830770, 830790, 830810, 830820, 830830, 830870, 831310, 830950, 830970, 831020, 831040, 831070, 831080, 831140, 831190, 831260,
                             831270, 831360, 831350, 831320, 840090, 840100, 840130, 840150, 840180, 840190, 840220, 840270, 840300, 840320, 840350, 840390,
                             150005, 820100, 840420, 840430, 840670, 840470, 840550, 840560, 840590, 840630, 840650, 840660, 840690, 874270, 840810, 840050,
                             840730, 840030, 840530, 840790, 840540, 820160, 840580, 840600, 840900, 842700, 842910, 850010, 850020, 850030, 850040, 850050,
                             850840, 875480, 840480, 850160, 850170, 840330, 850200, 843050, 850290, 850330, 830420, 850370, 850380, 850420, 852790, 850450,
                             850560, 850863, 850580, 850590, 850650, 850660, 850730, 850760, 850770, 874620, 841180, 842450, 850850, 850860, 850880, 850920,
                             850950, 850980, 851060, 830840, 851080, 840700, 851110, 874600, 851280, 875440, 851370, 851380, 851470, 851520, 851510, 851820,
                             860050, 860100, 861800, 860160, 860230, 860240, 860310, 874310, 860530, 830780, 873310, 860410, 860840, 860850, 874830, 860890,
                             150012, 860910, 500320, 861450, 862010, 150014, 875420, 870110, 862170, 861680, 860150, 150011, 150008, 150009, 150010, 159033,
                             159032, 159031, 159030, 159029, 159028, 159027, 159026, 159025, 159024, 159023, 159022, 159021, 159020, 159019, 159018, 159017,
                             159016, 159015, 159014, 159013, 159012, 159011, 159010, 159009, 159008, 159007, 159006, 159005, 159004, 159003, 159002, 159001,
                             159000, 125420, 125320, 125310, 125610, 560170, 125600, 125500, 900130, 861320, 852500, 852650, 510037, 852470, 831410, 871780,
                             874550, 862040, 861381, 852620, 861440, 852640, 517310, 861210, 852600, 873300, 861290, 831390, 860980, 150007, 852350, 852390,
                             852430, 911130, 840720, 830880, 872120, 560190, 831120, 852710, 150028, 850180, 562110, 852750, 852760, 850870, 150006, 150001,
                             851150, 150002, 150003, 840360, 840380, 830630, 831010, 840020, 840760, 840800, 840820, 840860, 841260, 861380, 830530, 830160,
                             830980, 830990, 861620, 852730, 550270, 830963, 582700, 860460, 852510, 830020, 831340, 850900, 150004, 852110, 120004, 840290,
                             862150, 871960, 871970, 871990, 872000, 872010, 872020, 861160, 830190, 500550, 861330, 500325, 870973, 870971);
    }

}
