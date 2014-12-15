/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.showorder;

import java.util.Arrays;
import org.junit.Test;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class AgencySearchOrderFromShowOrderTest {

    public AgencySearchOrderFromShowOrderTest() {
    }

    @Test
    public void testProvideAgenciesFor() {
        AgencySearchOrderFromShowOrder showOrder = mock(AgencySearchOrderFromShowOrder.class);
        doCallRealMethod().when(showOrder).provideAgenciesFor(anyInt());
        when(showOrder.fetchAgencies(100000)).thenReturn(Arrays.asList("870970", "150000"));
        when(showOrder.fetchAgencies(101000)).thenReturn(Arrays.asList("870970", "150000", "101000"));

        assertArrayEquals(new Object[]{100000, 870970, 150000}, showOrder.provideAgenciesFor(100000).toArray());

        assertArrayEquals(new Object[]{870970, 150000, 101000}, showOrder.provideAgenciesFor(101000).toArray());

    }

}
