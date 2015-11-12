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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.RecordMetaDataHistory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DateMatch
{
    private static final org.slf4j.Logger log = LoggerFactory.getLogger( DateMatch.class );

    public enum Match {

        Same,
        Before,
        BeforeOrEqual,
        After,
        AfterOrEqual,
    }

    /**
     * Find a record history data at or before the specified date.
     * If the record has multiple versions before the matching date,
     * the closest matching is returned
     *
     * @param date The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    public static RecordMetaDataHistory beforeOrSame( Date date, List<RecordMetaDataHistory> history ) {
        List<RecordMetaDataHistory> newestFirstHistory = new ArrayList<>( history );
        Collections.sort( newestFirstHistory, new Comparator<RecordMetaDataHistory>() {
            @Override
            public int compare( RecordMetaDataHistory o1, RecordMetaDataHistory o2 ) {
                return o2.getModified().compareTo( o1.getModified() );
            }
        } );
        for ( RecordMetaDataHistory element : newestFirstHistory ) {
            if ( element.getModified().getTime() <= date.getTime() ) {
                log.debug( "Found match {} for {}", element, date );
                return element;
            }
        }
        log.debug( "Found no match for {} in {}", date, history );
        return null;
    }

    /**
     * Find a record history data before the specified date.
     * If the record has multiple versions before the matching date,
     * the closest matching is returned
     *
     * @param date The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    public static RecordMetaDataHistory before( Date date, List<RecordMetaDataHistory> history ) {
        List<RecordMetaDataHistory> newestFirstHistory = new ArrayList<>( history );
        Collections.sort( newestFirstHistory, new Comparator<RecordMetaDataHistory>() {
            @Override
            public int compare( RecordMetaDataHistory o1, RecordMetaDataHistory o2 ) {
                return o2.getModified().compareTo( o1.getModified() );
            }
        } );
        for ( RecordMetaDataHistory element : newestFirstHistory ) {
            if ( element.getModified().getTime() < date.getTime() ) {
                log.debug( "Found match {} for {}", element, date );
                return element;
            }
        }
        log.debug( "Found no match for {} in {}", date, history );
        return null;
    }

    /**
     * Find a record history data at or after the specified date.
     * If the record has multiple versions after the matching date,
     * the closest matching is returned
     *
     * @param date The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    public static RecordMetaDataHistory afterOrSame( Date date, List<RecordMetaDataHistory> history ) {
        List<RecordMetaDataHistory> oldestFirstHistory = new ArrayList<>( history );
        Collections.sort( oldestFirstHistory, new Comparator<RecordMetaDataHistory>() {
            @Override
            public int compare( RecordMetaDataHistory o1, RecordMetaDataHistory o2 ) {
                return o1.getModified().compareTo( o2.getModified() );
            }
        } );
        for ( RecordMetaDataHistory element : oldestFirstHistory ) {
            if ( element.getModified().getTime() >= date.getTime() ) {
                log.debug( "Found match {} for {}", element, date );
                return element;
            }
        }
        log.debug( "Found no match for {} in {}", date, history );
        return null;
    }

    /**
     * Find a record history data after the specified date.
     * If the record has multiple versions after the matching date,
     * the closest matching is returned
     *
     * @param date The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    public static RecordMetaDataHistory after( Date date, List<RecordMetaDataHistory> history ) {
        List<RecordMetaDataHistory> oldestFirstHistory = new ArrayList<>( history );
        Collections.sort( oldestFirstHistory, new Comparator<RecordMetaDataHistory>() {
            @Override
            public int compare( RecordMetaDataHistory o1, RecordMetaDataHistory o2 ) {
                return o1.getModified().compareTo( o2.getModified() );
            }
        } );
        for ( RecordMetaDataHistory element : oldestFirstHistory ) {
            if ( element.getModified().getTime() > date.getTime() ) {
                log.debug( "Found match {} for {}", element, date );
                return element;
            }
        }
        log.debug( "Found no match for {} in {}", date, history );
        return null;
    }

    /**
     * Find a historic record with the specified date.
     * If multiple records have the matching date,
     * the first matching in the list is returned.
     *
     * @param date The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    public static RecordMetaDataHistory same( Date date, List<RecordMetaDataHistory> history ) {
        for ( RecordMetaDataHistory element : history ) {
            if ( element.getModified().getTime() == date.getTime() ) {
                log.debug( "Found match {} for {}", element, date );
                return element;
            }
        }
        log.debug( "Found no match for {} in {}", date, history );
        return null;
    }

}