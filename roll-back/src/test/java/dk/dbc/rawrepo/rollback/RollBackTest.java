/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author thp
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
