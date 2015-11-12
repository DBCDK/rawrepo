/*
 This file is part of opensearch.
 Copyright © 2013, Dansk Bibliotekscenter a/s,
 Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043

 opensearch is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 opensearch is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with opensearch.  If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class RollBackIT {

    private static final int AGENCY_ID = 100000;
    private static final String BIB_RECORD_ID_1 = "a bcd efg h";
    private static final String BIB_RECORD_ID_2 = "q wer tyu i";

    private final static Date NOW;
    private final static Date DAY_1;
    private final static Date DAY_2;
    private final static Date DAY_3;
    private final static Date DAY_4;
    private final static Date DAY_5;
    private final static Date DAY_6;
    private final static Date DAY_7;

    static {
        Calendar cal = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        NOW = new Date();
        cal.setTime( NOW );
        cal.add( Calendar.DATE, -1 );
        DAY_7 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_6 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_5 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_4 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_3 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_2 = cal.getTime();
        cal.add( Calendar.DATE, -1 );
        DAY_1 = cal.getTime();
    }

    private Connection connection;
    private String jdbc;

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        String port = System.getProperty( "postgresql.port" );
        jdbc = "jdbc:postgresql://localhost:" + port + "/rawrepo";
        Properties properties = new Properties();

        connection = DriverManager.getConnection( jdbc, properties );
        connection.prepareStatement( "SET log_statement = 'all';" ).execute();
        resetDatabase();
    }

    @After
    public void teardown() throws SQLException {
        connection.close();
    }

    void resetDatabase() throws SQLException {
        connection.prepareStatement( "DELETE FROM relations" ).execute();
        connection.prepareStatement( "DELETE FROM records" ).execute();
        connection.prepareStatement( "DELETE FROM records_archive" ).execute();
        connection.prepareStatement( "DELETE FROM queue" ).execute();
        connection.prepareStatement( "DELETE FROM queuerules" ).execute();
        connection.prepareStatement( "DELETE FROM queueworkers" ).execute();
        connection.prepareStatement( "DELETE FROM jobdiag" ).execute();
    }

    @Test
    public void testHistoricRecord() throws SQLException, ClassNotFoundException, RawRepoException {

        RawRepoDAO dao = RawRepoDAO.newInstance( connection );
        connection.setAutoCommit( false );
        Record record;
        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        record.setContent( "Version 1".getBytes() );
        record.setMimeType( "text/plain" );
        record.setDeleted( false );
        record.setModified( DAY_1 );
        dao.saveRecord( record );
        connection.commit();

        connection.setAutoCommit( false );
        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        record.setContent( "Version 2".getBytes() );
        record.setMimeType( "text/not-so-plain" );
        record.setDeleted( false );
        record.setModified( DAY_3 );
        dao.saveRecord( record );
        connection.commit();

        connection.setAutoCommit( false );
        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        record.setContent( "Version 3".getBytes() );
        record.setMimeType( "text/really-plain" );
        record.setDeleted( true );
        record.setModified( DAY_5 );
        dao.saveRecord( record );
        connection.commit();

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( recordHistory.toString(), 3, recordHistory.size() );

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "Version 3", new String ( record.getContent(), StandardCharsets.UTF_8) );

        RollBack.rollbackRecord( connection, new RecordId(BIB_RECORD_ID_1, AGENCY_ID ), DAY_4, DateMatch.Match.Before, RollBack.State.Rollback, null );
        connection.commit();

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "Version 2", new String ( record.getContent(), StandardCharsets.UTF_8) );

        RollBack.rollbackRecord( connection, new RecordId(BIB_RECORD_ID_1, AGENCY_ID ), DAY_4, DateMatch.Match.After, RollBack.State.Rollback, null );
        connection.commit();

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "Version 3", new String ( record.getContent(), StandardCharsets.UTF_8) );
    }
    
    @Test
    public void testBulkAgency_with2Records() throws SQLException, ClassNotFoundException, RawRepoException {

        RawRepoDAO dao = RawRepoDAO.newInstance( connection );
        connection.setAutoCommit( false );
        Record record;
        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        record.setContent( "Rec 1 Version 1".getBytes() );
        record.setMimeType( "text/plain" );
        record.setDeleted( false );
        record.setModified( DAY_1 );
        dao.saveRecord( record );
        connection.commit();

        connection.setAutoCommit( false );
        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        record.setContent( "Rec 1 Version 2".getBytes() );
        record.setMimeType( "text/not-so-plain" );
        record.setDeleted( false );
        record.setModified( DAY_4 );
        dao.saveRecord( record );
        connection.commit();

        connection.setAutoCommit( false );
        record = dao.fetchRecord( BIB_RECORD_ID_2, AGENCY_ID );
        record.setContent( "Rec 2 Version 1".getBytes() );
        record.setMimeType( "text/really-plain" );
        record.setDeleted( true );
        record.setModified( DAY_3 );
        dao.saveRecord( record );
        connection.commit();

        connection.setAutoCommit( false );
        record = dao.fetchRecord( BIB_RECORD_ID_2, AGENCY_ID );
        record.setContent( "Rec 2 Version 2".getBytes() );
        record.setMimeType( "text/really-plain" );
        record.setDeleted( true );
        record.setModified( DAY_5 );
        dao.saveRecord( record );
        connection.commit();

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( recordHistory.toString(), 2, recordHistory.size() );
        recordHistory = dao.getRecordHistory( BIB_RECORD_ID_2, AGENCY_ID );
        assertEquals( recordHistory.toString(), 2, recordHistory.size() );

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "Rec 1 Version 2", new String ( record.getContent(), StandardCharsets.UTF_8) );

        record = dao.fetchRecord( BIB_RECORD_ID_2, AGENCY_ID );
        assertEquals( "Rec 2 Version 2", new String ( record.getContent(), StandardCharsets.UTF_8) );

        RollBack.rollbackAgency( connection, AGENCY_ID, DAY_3, DateMatch.Match.Before, RollBack.State.Rollback, null );
        connection.commit();

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "First record is rolled back",  "Rec 1 Version 1", new String ( record.getContent(), StandardCharsets.UTF_8) );

        record = dao.fetchRecord( BIB_RECORD_ID_2, AGENCY_ID );
        assertEquals( "Second record is not rolled back", "Rec 2 Version 2", new String ( record.getContent(), StandardCharsets.UTF_8) );

        RollBack.rollbackAgency( connection, AGENCY_ID, DAY_4, DateMatch.Match.Before, RollBack.State.Rollback, null );
        connection.commit();

        record = dao.fetchRecord( BIB_RECORD_ID_1, AGENCY_ID );
        assertEquals( "First record is rolled back",  "Rec 1 Version 1", new String ( record.getContent(), StandardCharsets.UTF_8) );

        record = dao.fetchRecord( BIB_RECORD_ID_2, AGENCY_ID );
        assertEquals( "Second record is rolled back", "Rec 2 Version 1", new String ( record.getContent(), StandardCharsets.UTF_8) );
    }

}
