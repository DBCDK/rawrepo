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

import dk.dbc.rawrepo.RecordMetaDataHistory;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class DateMatch {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DateMatch.class);

    private final static Comparator<RecordMetaDataHistory> newestFirst = new Comparator<RecordMetaDataHistory>() {
        @Override
        public int compare(RecordMetaDataHistory o1, RecordMetaDataHistory o2) {
            return o2.getModified().compareTo(o1.getModified());
        }
    };

    private final static Comparator<RecordMetaDataHistory> oldestFirst = new Comparator<RecordMetaDataHistory>() {
        @Override
        public int compare(RecordMetaDataHistory o1, RecordMetaDataHistory o2) {
            return o1.getModified().compareTo(o2.getModified());
        }
    };

    public enum Match {

        Equal("Match against records with the exact specified timestamp", "="),
        Before("Match against the record version with the closest timestamp before the specified date", "<"),
        BeforeOrEqual("Match against the record version with the closest timestamp before, or equal to, the specified date", "<="),
        After("Match against the record version with the closest timestamp after the specified date", ">"),
        AfterOrEqual("Match against the record version with the closest timestamp after, or equal to, the specified date", ">=");

        private final String description;
        private final String operator;

        Match(String description, String operator) {
            this.description = description;
            this.operator = operator;
        }

        public String getDescription() {
            return name() + " - " + description;
        }

        public String getOperator() {
            return operator;
        }
    }

    /**
     * Find a record history data at or before the specified date. If the record
     * has multiple versions before the matching date, the closest matching is
     * returned
     *
     * @param date    The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    static RecordMetaDataHistory beforeOrSame(Instant date, List<RecordMetaDataHistory> history) {
        List<RecordMetaDataHistory> newestFirstHistory = new ArrayList<>(history);
        Collections.sort(newestFirstHistory, newestFirst);
        for (RecordMetaDataHistory element : newestFirstHistory) {
            if (element.getModified().isBefore(date) || element.getModified().equals(date)) {
                log.debug("Found match {} for {}", element, date);
                return element;
            }
        }
        log.debug("Found no match for {} in {}", date, history);
        return null;
    }

    /**
     * Find a record history data before the specified date. If the record has
     * multiple versions before the matching date, the closest matching is
     * returned
     *
     * @param date    The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    static RecordMetaDataHistory before(Instant date, List<RecordMetaDataHistory> history) {
        List<RecordMetaDataHistory> newestFirstHistory = new ArrayList<>(history);
        Collections.sort(newestFirstHistory, newestFirst);
        for (RecordMetaDataHistory element : newestFirstHistory) {
            if (element.getModified().isBefore(date)) {
                log.debug("Found match {} for {}", element, date);
                return element;
            }
        }
        log.debug("Found no match for {} in {}", date, history);
        return null;
    }

    /**
     * Find a record history data at or after the specified date. If the record
     * has multiple versions after the matching date, the closest matching is
     * returned
     *
     * @param date    The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    static RecordMetaDataHistory afterOrSame(Instant date, List<RecordMetaDataHistory> history) {
        List<RecordMetaDataHistory> oldestFirstHistory = new ArrayList<>(history);
        Collections.sort(oldestFirstHistory, oldestFirst);
        for (RecordMetaDataHistory element : oldestFirstHistory) {
            if (element.getModified().isAfter(date) || element.getModified().equals(date)) {
                log.debug("Found match {} for {}", element, date);
                return element;
            }
        }
        log.debug("Found no match for {} in {}", date, history);
        return null;
    }

    /**
     * Find a record history data after the specified date. If the record has
     * multiple versions after the matching date, the closest matching is
     * returned
     *
     * @param date    The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    static RecordMetaDataHistory after(Instant date, List<RecordMetaDataHistory> history) {
        List<RecordMetaDataHistory> oldestFirstHistory = new ArrayList<>(history);
        Collections.sort(oldestFirstHistory, oldestFirst);
        for (RecordMetaDataHistory element : oldestFirstHistory) {
            if (element.getModified().isAfter(date)) {
                log.debug("Found match {} for {}", element, date);
                return element;
            }
        }
        log.debug("Found no match for {} in {}", date, history);
        return null;
    }

    /**
     * Find a historic record with the specified date. If multiple records have
     * the matching date, the first matching in the list is returned.
     *
     * @param date    The date to match against
     * @param history List of record history data
     * @return The first matching record or null if no record matches.
     */
    static RecordMetaDataHistory equal(Instant date, List<RecordMetaDataHistory> history) {
        for (RecordMetaDataHistory element : history) {
            log.debug("Comparing {} to {}", element.getModified(), date);
            if (element.getModified().equals(date)) {
                log.debug("Found match {} for {}", element, date);
                return element;
            }
        }
        log.debug("Found no match for {} in {}", date, history);
        return null;
    }

}
