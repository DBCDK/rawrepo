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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RollBack {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger( RollBack.class );

    public enum State {

        Keep,
        Rollback,
        Delete,
        Active,
    }

    public static boolean rollbackRecord( Connection connection, RecordId id, Date matchDate, DateMatch.Match matchType, State state, String queueRole ) throws RawRepoException {
        log.info( "Rolling record {} back to {}", id, matchDate );
        RawRepoDAO dao = RawRepoDAO.newInstance( connection );
        boolean modified = rollbackRecord( dao, id, matchDate, matchType, state );
        if ( modified ) {
            queueRecord( dao, id, queueRole );
        }
        return modified;
    }

    public static boolean rollbackRecord( RawRepoDAO dao, RecordId id, Date matchDate, DateMatch.Match matchType, State state ) throws RawRepoException {

        List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory( id.getBibliographicRecordId(), id.getAgencyId() );

        RecordMetaDataHistory matching = null;
        String matchOperator = "";

        switch ( matchType ) {
            case Same: {
                matching = DateMatch.same( matchDate, recordHistory );
                matchOperator = "=";
                break;
            }
            case Before: {
                matching = DateMatch.before( matchDate, recordHistory );
                matchOperator = "<";
                break;
            }
            case BeforeOrEqual: {
                matching = DateMatch.beforeOrSame( matchDate, recordHistory );
                matchOperator = "<=";
                break;
            }
            case After: {
                matching = DateMatch.after( matchDate, recordHistory );
                matchOperator = ">";
                break;
            }
            case AfterOrEqual: {
                matching = DateMatch.afterOrSame( matchDate, recordHistory );
                matchOperator = "=>";
                break;
            }
            default: {
                log.error( "Unsupported match type '{}'", matchType );
                break;
            }
        }
        if ( matching != null ) {
            log.debug( "Rolling record {} back to {}", id, matchDate );

            Record currentRecord = dao.fetchRecord( id.getBibliographicRecordId(), id.getAgencyId() );
            Record historicRecord = dao.getHistoricRecord( matching );
            boolean deleted;
            switch ( state ) {
                case Keep: {
                    deleted = currentRecord.isDeleted();
                    break;
                }
                case Rollback: {
                    deleted = historicRecord.isDeleted();
                    break;
                }
                case Delete: {
                    deleted = true;
                    break;
                }
                case Active: {
                    deleted = false;
                    break;
                }
                default: {
                    log.error( "Unsupported match type '{}'", matchType );
                    deleted = historicRecord.isDeleted();
                    break;
                }
            }
            log.debug( "Old deleted state {}, new deleted state {}", historicRecord.isDeleted(), deleted );
            if ( currentRecord.isDeleted() && !historicRecord.isDeleted() ) {
                log.warn( "Undeleting record {}. Relations are not restored", id );
            }
            historicRecord.setDeleted( deleted );
            historicRecord.setModified( new Date() );
            historicRecord.setTrackingId( "Rollback:" + matchOperator + matchDate );
            dao.saveRecord( historicRecord );
            return true;
        }
        else {
            log.debug( "No matching history for record {} to {}", id, matchDate );
            return false;
        }
    }

    static void queueRecord( RawRepoDAO dao, RecordId id, String role ) throws RawRepoException {
        String mimetype = dao.getMimeTypeOf( id.getBibliographicRecordId(), id.getAgencyId() );
        dao.changedRecord( role, id, mimetype );
    }

    static Set<String> getRecordIds( Connection connection, int agencyId ) throws SQLException {
        Set<String> set = new HashSet<>();
        try ( PreparedStatement stmt = connection.prepareStatement( "SELECT bibliographicrecordid FROM records WHERE agencyid = ?" ) ) {
            stmt.setInt( 1, agencyId );
            try ( ResultSet resultSet = stmt.executeQuery() ) {
                while ( resultSet.next() ) {
                    String recordId = resultSet.getString( 1 );
                    log.trace( "Adding agency {} record '{}' ", agencyId, recordId );
                    set.add( recordId );
                }
            }
        }
        return set;
    }

    static void rollbackRecords( int agencyId, Set<String> ids, RawRepoDAO dao, Date matchDate, DateMatch.Match matchType, State state ) {
        int success = 0;
        int skipped = 0;
        int failed = 0;

        for ( String id : ids ) {
            RecordId recordId = new RecordId( id, agencyId );
            try {
                if ( rollbackRecord( dao, recordId, matchDate, matchType, state ) ) {
                    success++;
                }
                else {
                    skipped++;
                }
            }
            catch ( RawRepoException ex ) {
                failed++;
                log.error( "Failed to rollback record " + recordId, ex );
            }
            if ( ( success + skipped + failed ) % 1000 == 0 ) {
                log.info( "Rolled back {}, skipped {}, failed {}", success, skipped, failed );
            }
        }
        log.info( "Rolled back {}, skipped {}, failed {}. Done.", success, skipped, failed );
    }

    static void queueRecords( RawRepoDAO dao, int agencyId, Set<String> ids, String role ) throws RawRepoException {
        int no = 0;
        log.info( "Queing {} ids", ids.size() );

        for ( String id : ids ) {
            queueRecord( dao, new RecordId( id, agencyId ), role );
            if ( ++no % 1000 == 0 ) {
                log.info( "Queued {}", no );
            }
        }
        log.info( "Queued {}", no );
    }

    public static void rollbackAgency( Connection connection, int agencyId, Date matchDate, DateMatch.Match matchType, State state, String queueRole ) throws RawRepoException, SQLException {

        RawRepoDAO dao = RawRepoDAO.newInstance( connection );

        log.info( "Identifying records for {}", agencyId );
        Set<String> ids = getRecordIds( connection, agencyId );

        log.info( "Rolling back {} records for agency {}. Matching {} as '{}'",
                ids.size(), agencyId, matchDate, matchType );

        rollbackRecords( agencyId, ids, dao, matchDate, matchType, state );
        if ( queueRole != null ) {
            queueRecords( dao, agencyId, ids, queueRole );
        }
    }

}