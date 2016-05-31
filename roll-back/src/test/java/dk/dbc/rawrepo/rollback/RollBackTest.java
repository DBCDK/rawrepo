/*
 * dbc-rawrepo-rollback
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-rollback.
 *
 * dbc-rawrepo-rollback is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-rollback is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-rollback.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class RollBackTest {

    Record mockRecord( boolean isDeleted ) {
        Record mockRec = mock( Record.class );
        when( mockRec.isDeleted() ).thenReturn( isDeleted );
        return mockRec;
    }

    @Test
    public void testGetNewState() {
        assertFalse( RollBack.getNewDeleted( RollBack.State.Active, mockRecord( false ), mockRecord( false ) ) );
        assertTrue( RollBack.getNewDeleted( RollBack.State.Delete, mockRecord( true ), mockRecord( true ) ) );

        assertTrue( RollBack.getNewDeleted( RollBack.State.Keep, mockRecord( true ), mockRecord( false ) ) );
        assertFalse( RollBack.getNewDeleted( RollBack.State.Keep, mockRecord( false ), mockRecord( true ) ) );

        assertFalse( RollBack.getNewDeleted( RollBack.State.Rollback, mockRecord( true ), mockRecord( false ) ) );
        assertTrue( RollBack.getNewDeleted( RollBack.State.Rollback, mockRecord( false ), mockRecord( true ) ) );
    }

}
