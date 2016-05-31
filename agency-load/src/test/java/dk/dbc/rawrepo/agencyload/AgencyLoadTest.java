/*
 * dbc-rawrepo-agency-load
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-load.
 *
 * dbc-rawrepo-agency-load is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-load is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-load.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencyload;

import java.io.InputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.*;
//import static org.junit.Assert.*;

/**
 *
 * @author DBC <dbc.dk>
 */
public class AgencyLoadTest {

    public AgencyLoadTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test() throws Exception {
        AgencyLoad agencyLoad = mock(AgencyLoad.class);
        agencyLoad.createMetrics();

        doCallRealMethod().when(agencyLoad).load((InputStream) anyObject());
        InputStream is = getClass().getClassLoader().getResourceAsStream("recs.xml");

        agencyLoad.load(is);

        verify(agencyLoad, times(1)).store(any(byte[].class), eq(191919), eq("01704036"), isNull(String.class), anyBoolean());
        verify(agencyLoad, times(1)).store(any(byte[].class), eq(191919), eq("01990810"), eq("50155919"), anyBoolean());
    }

}
