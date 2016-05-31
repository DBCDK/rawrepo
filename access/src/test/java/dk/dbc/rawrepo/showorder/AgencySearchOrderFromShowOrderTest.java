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
package dk.dbc.rawrepo.showorder;

import java.util.Arrays;
import org.junit.Test;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

/**
 *
 * @author DBC <dbc.dk>
 */
public class AgencySearchOrderFromShowOrderTest {

    public AgencySearchOrderFromShowOrderTest() {
    }

    @Test
    public void testProvideAgenciesFor() throws Exception {
        AgencySearchOrderFromShowOrder showOrder = mock(AgencySearchOrderFromShowOrder.class);
        doCallRealMethod().when(showOrder).provide(anyInt());
        when(showOrder.fetchOrder(100000)).thenReturn(Arrays.asList("870970", "150000"));
        when(showOrder.fetchOrder(101000)).thenReturn(Arrays.asList("870970", "150000", "101000"));

        assertArrayEquals(new Object[]{100000, 870970, 150000}, showOrder.provide(100000).toArray());

        assertArrayEquals(new Object[]{870970, 150000, 101000}, showOrder.provide(101000).toArray());

    }

}
