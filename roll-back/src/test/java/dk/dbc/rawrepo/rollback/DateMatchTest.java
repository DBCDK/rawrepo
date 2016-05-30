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

import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.junit.Test;
import static org.junit.Assert.*;

public class DateMatchTest {

    private final static SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'" );
    private final static Date DAY_1;
    private final static Date DAY_2;
    private final static Date DAY_3;
    private final static Date DAY_4;
    private final static Date DAY_5;

    static {
        try {
            dateFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
            DAY_1 = dateFormat.parse( "2015-01-01T00:00:00.000Z" );
            DAY_2 = dateFormat.parse( "2015-02-02T00:00:00.000Z" );
            DAY_3 = dateFormat.parse( "2015-03-03T00:00:00.000Z" );
            DAY_4 = dateFormat.parse( "2015-04-04T00:00:00.000Z" );
            DAY_5 = dateFormat.parse( "2015-05-05T00:00:00.000Z" );
        }
        catch ( ParseException ex ) {
            throw new RuntimeException( ex );
        }
    }

    List<RecordMetaDataHistory> historyData;

    List<RecordMetaDataHistory> createHistoryData( Date... modifiedDates ) {
        List<RecordMetaDataHistory> data = new ArrayList<>();

        RecordId id = new RecordId("RECID", 100000 );
        Timestamp created = new Timestamp( System.currentTimeMillis() );

        for ( Date modifiedDate : modifiedDates ) {
            data.add( new RecordMetaDataHistory( id, true, "", created, new Timestamp( modifiedDate.getTime() ), ""));
        }

        return data;
    }

    @Test
    public void equal_whenNoMatch() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertNull(DateMatch.equal(DAY_4, historyData) );
    }

    @Test
    public void equal_whenExactMatch() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertEquals( DAY_1, DateMatch.equal(DAY_1, historyData).getModified() );
        assertEquals( DAY_3, DateMatch.equal(DAY_3, historyData).getModified() );
        assertEquals( DAY_5, DateMatch.equal(DAY_5, historyData).getModified() );
    }

    @Test
    public void before_whenNoMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertNull(DateMatch.before(DAY_1, historyData) );
    }

    @Test
    public void before_whenExactMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertEquals( DAY_2, DateMatch.before(DAY_3, historyData).getModified() );
        historyData = createHistoryData(DAY_4, DAY_2, DAY_3);
        assertEquals( DAY_2, DateMatch.before(DAY_3, historyData).getModified() );
    }

    @Test
    public void before_whenTwoAreLess() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertEquals( DAY_3, DateMatch.before(DAY_4, historyData).getModified() );
        historyData = createHistoryData(DAY_5, DAY_3, DAY_1);
        assertEquals( DAY_3, DateMatch.before(DAY_4, historyData).getModified() );
    }

    @Test
    public void beforeOrSame_whenNoMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertNull(DateMatch.beforeOrSame(DAY_1, historyData) );
    }

    @Test
    public void beforeOrSame_whenExactMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertEquals( DAY_3, DateMatch.beforeOrSame(DAY_3, historyData).getModified() );
        historyData = createHistoryData(DAY_4, DAY_2, DAY_3);
        assertEquals( DAY_3, DateMatch.beforeOrSame(DAY_3, historyData).getModified() );
    }

    @Test
    public void beforeOrSame_whenTwoAreLess() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertEquals( DAY_3, DateMatch.beforeOrSame(DAY_4, historyData).getModified() );
        historyData = createHistoryData(DAY_5, DAY_3, DAY_1);
        assertEquals( DAY_3, DateMatch.beforeOrSame(DAY_4, historyData).getModified() );
    }

    @Test
    public void afterOrSame_whenNoMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertNull(DateMatch.afterOrSame(DAY_5, historyData) );
    }

    @Test
    public void afterOrSame_whenExactMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertEquals( DAY_3, DateMatch.afterOrSame(DAY_3, historyData).getModified() );
        
        historyData = createHistoryData(DAY_3, DAY_2, DAY_4);
        assertEquals( DAY_3, DateMatch.afterOrSame(DAY_3, historyData).getModified() );
    }

    @Test
    public void afterOrSame_whenTwoAreHigher() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertEquals( DAY_3, DateMatch.afterOrSame(DAY_2, historyData).getModified() );

        historyData = createHistoryData(DAY_5, DAY_3, DAY_1);
        assertEquals( DAY_3, DateMatch.afterOrSame(DAY_2, historyData).getModified() );
    }

    @Test
    public void after_whenNoMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertNull(DateMatch.after(DAY_5, historyData) );
    }

    @Test
    public void after_whenExactMatch() {
        historyData = createHistoryData(DAY_2, DAY_3, DAY_4);
        assertEquals( DAY_4, DateMatch.after(DAY_3, historyData).getModified() );

        historyData = createHistoryData(DAY_3, DAY_2, DAY_4);
        assertEquals( DAY_4, DateMatch.after(DAY_3, historyData).getModified() );
    }

    @Test
    public void after_whenTwoAreHigher() {
        historyData = createHistoryData(DAY_1, DAY_3, DAY_5);
        assertEquals( DAY_3, DateMatch.after(DAY_2, historyData).getModified() );

        historyData = createHistoryData(DAY_5, DAY_3, DAY_1);
        assertEquals( DAY_3, DateMatch.after(DAY_2, historyData).getModified() );
    }

}
