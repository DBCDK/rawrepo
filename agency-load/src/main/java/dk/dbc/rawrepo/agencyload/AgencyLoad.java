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

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyLoad implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AgencyLoad.class);

    private final Connection connection;
    private final RawRepoDAO dao;
    private final List<Integer> agencies;
    private final Integer commonAgency;
    private final String role;
    private final Set<RecordId> bibliographicRecordIds;
    private final Map<RecordId, String> parentRelations;

    private MetricRegistry metrics;
    private JmxReporter reporter;
    Timer recordsProcessed;
    Timer recordsRelated;
    Timer recordsQueued;
    Counter enrichmentRecords;
    Counter parentRelationRecords;
    Counter deletedRecords;
    Counter errorRecords;
    Counter relationErrors;
    Counter queueErrors;
    private Timer setRelations;
    private Timer recordExists;
    private Timer saveRecord;
    private Timer fetchRecord;

    public void timingStart() {
        reporter.start();
    }

    public void timingStop() {
        reporter.stop();
    }

    AgencyLoad(String db, List<Integer> agencies, Integer commonAgency, String role, boolean useTransaction) throws RawRepoException, SQLException {
        this.connection = getConnection(db);
        if (useTransaction) {
            this.connection.setAutoCommit(false);
        }
        this.dao = RawRepoDAO.builder(connection).build();
        this.agencies = agencies;
        this.commonAgency = commonAgency;
        this.role = role;
        this.bibliographicRecordIds = new HashSet<>();
        this.parentRelations = new HashMap<>();

        createMetrics();

    }

    final void createMetrics() {
        this.metrics = new MetricRegistry();
        this.reporter = JmxReporter.forRegistry(metrics).build();
        this.recordsProcessed = metrics.timer(MetricRegistry.name(AgencyLoad.class, "recordsProcessed"));
        this.recordExists = metrics.timer(MetricRegistry.name(AgencyLoad.class, "recordExists"));
        this.fetchRecord = metrics.timer(MetricRegistry.name(AgencyLoad.class, "fetchRecord"));
        this.saveRecord = metrics.timer(MetricRegistry.name(AgencyLoad.class, "saveRecord"));
        this.setRelations = metrics.timer(MetricRegistry.name(AgencyLoad.class, "setRelations"));
        this.recordsRelated = metrics.timer(MetricRegistry.name(AgencyLoad.class, "recordsRelated"));
        this.recordsQueued = metrics.timer(MetricRegistry.name(AgencyLoad.class, "recordsQueued"));
        this.enrichmentRecords = metrics.counter("enrichmentRecords");
        this.parentRelationRecords = metrics.counter("parentRelationRecords");
        this.deletedRecords = metrics.counter("deletedRecords");
        this.errorRecords = metrics.counter("errorRecords");
        this.relationErrors = metrics.counter("relationErrors");
        this.queueErrors = metrics.counter("queueErrors");
    }

    @Override
    public void close() {
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException ex) {
            log.warn("Cannot rollback db connection");
        }
        try {
            connection.close();
        } catch (SQLException ex) {
            log.warn("Cannot close db connection");
        }
    }

    public void store(byte[] xml, int agencyId, String bibliographicRecordId, String parentBibliographicRecordId, boolean isDeleted) {
        try (Timer.Context time = recordsProcessed.time()) {
            long processedRecords = recordsProcessed.getCount();
            if (processedRecords % 1000 == 0) {
                log.info("Processing record: " + processedRecords);
            }
            try {
                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
                log.debug("Processing: " + recordId);
                Record record;
                try (Timer.Context time1 = fetchRecord.time()) {
                    record = dao.fetchRecord(bibliographicRecordId, agencyId);
                }

                bibliographicRecordIds.add(recordId);
                HashSet<RecordId> relations = new HashSet<>();
                if (!record.isOriginal()) {
                    try (Timer.Context time1 = setRelations.time()) {
                        dao.setRelationsFrom(recordId, relations);
                    }
                }

                record.setContent(xml);
                record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
                record.setDeleted(isDeleted);

                if (isDeleted) {
                    deletedRecords.inc();
                    try (Timer.Context time1 = saveRecord.time()) {
                        dao.saveRecord(record);
                    }
                    return;
                }

                for (Integer sibling : agencies) {
                    boolean recordExistsVal;
                    try (Timer.Context time1 = recordExists.time()) {
                        recordExistsVal = dao.recordExists(bibliographicRecordId, sibling);
                    }
                    if (recordExistsVal) {
                        enrichmentRecords.inc();
                        log.debug("agencyId = " + agencyId + "; bibliographicRecordId = " + bibliographicRecordId + "; siblingAgency = " + sibling);
                        record.setMimeType(MarcXChangeMimeType.ENRICHMENT);
                        try (Timer.Context time1 = saveRecord.time()) {
                            dao.saveRecord(record);
                        }
                        relations.add(new RecordId(bibliographicRecordId, sibling));
                        try (Timer.Context time1 = setRelations.time()) {
                            dao.setRelationsFrom(recordId, relations);
                        }
                        return;
                    }
                }
                record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
                try (Timer.Context time1 = saveRecord.time()) {
                    dao.saveRecord(record);
                }
                if (parentBibliographicRecordId != null && !parentBibliographicRecordId.isEmpty()) {
                    parentRelationRecords.inc();
                    parentRelations.put(recordId, parentBibliographicRecordId);
                }
            } catch (RawRepoException ex) {
                log.error("Error processing record: " + bibliographicRecordId +
                          " from " + agencyId + " got: " + ex.getMessage());
                errorRecords.inc();
            }
        }
    }

    boolean load(InputStream is) throws ParserConfigurationException, SAXException, IOException {
        MarcXProcessor marcXProcessor = new MarcXProcessor() {

                   String bibliographicRecordId;
                   int agencyId;
                   String parentBibliographicRecordId;
                   boolean isDeleted;

                   void reset() {
                       bibliographicRecordId = null;
                       agencyId = -1;
                       parentBibliographicRecordId = null;
                       isDeleted = false;
                   }

                   @Override
                   public void marcxContent(String pos, String data) {
                       switch (pos) {
                           case "001a":
                               bibliographicRecordId = data;
                               break;
                           case "001b":
                               agencyId = Integer.parseInt(data, 10);
                               break;
                           case "014a":
                               parentBibliographicRecordId = data;
                               break;
                           case "004r":
                               isDeleted = data.equals("d");
                               break;
                           default:
                               break;
                       }
                   }

                   @Override
                   public void marcxXml(final byte[] xml) {
                       log.trace("agencyId = " + agencyId);
                       log.trace("bibliographicRecordId = " + bibliographicRecordId);
                       log.trace("parentBibliographicRecordId = " + parentBibliographicRecordId);
                       log.trace("isDeleted = " + isDeleted);
                       store(xml, agencyId, bibliographicRecordId, parentBibliographicRecordId, isDeleted);
                   }

                   @Override
                   public MarcXBlock makeMarcXBlock() {
                       reset();
                       MarcXBlock marcXBlock = new MarcXBlock(this);
                       marcXBlock.addPrefix("marcx", "info:lc/xmlns/marcxchange-v1");
                       return marcXBlock;
                   }

               };

        MarcXParser.parse(is, marcXProcessor);
        return errorRecords.getCount() == 0;
    }

    boolean buildParentRelations() {
        try (Timer.Context time = recordsRelated.time()) {
            log.info("Building parent relations (" + parentRelations.size() + ")");
            boolean success = true;
            int cnt = 0;
            for (Map.Entry<RecordId, String> entry : parentRelations.entrySet()) {
                if (++cnt % 1000 == 0) {
                    log.info("Processing relation: " + cnt);
                }
                RecordId recordId = entry.getKey();
                String parentBibliographicRecordId = entry.getValue();
                log.debug("relating: " + recordId + " to: " + parentBibliographicRecordId);
                try {
                    Set<RecordId> relations = dao.getRelationsFrom(recordId);
                    if (commonAgency != null && dao.recordExists(parentBibliographicRecordId, commonAgency)) {
                        relations.add(new RecordId(parentBibliographicRecordId, commonAgency));
                        dao.setRelationsFrom(recordId, relations);
                    } else if (dao.recordExists(parentBibliographicRecordId, recordId.getAgencyId())) {
                        relations.add(new RecordId(parentBibliographicRecordId, recordId.getAgencyId()));
                        dao.setRelationsFrom(recordId, relations);
                    } else {
                        log.error("Cannot create parent relation to: " + parentBibliographicRecordId +
                                  " from " + recordId.getBibliographicRecordId() +
                                  " for agency " + recordId.getAgencyId() +
                                  " parent not found");
                        success = false;
                        relationErrors.inc();
                    }
                } catch (RawRepoException ex) {
                    log.error("Error relating record: " + parentBibliographicRecordId +
                              " from " + recordId.getAgencyId() +
                              " to: " + parentBibliographicRecordId +
                              " got: " + ex.getMessage());
                    success = false;
                    relationErrors.inc();
                }
            }
            return success;
        }

    }

    boolean queue() {
        boolean success = true;
        if (role != null) {
            try (Timer.Context time = recordsQueued.time()) {
                log.info("Queueing");
                int cnt = 0;
                for (RecordId recordId : bibliographicRecordIds) {
                    if (++cnt % 1000 == 0) {
                        log.info("Queueing: " + cnt);
                    }
                    try {
                        dao.changedRecord(role, recordId, MarcXChangeMimeType.MARCXCHANGE);
                    } catch (RawRepoException ex) {
                        log.error("Error queueing record: " + recordId.getBibliographicRecordId() +
                                  " from " + recordId.getAgencyId() + " got: " + ex.getMessage());
                        success = false;
                        queueErrors.inc();
                    }
                }
                log.info("Queued " + cnt);
            }
        }
        return success;
    }

    void status() {
        log.info("Processed Records: " + recordsProcessed.getCount());
        log.info("Deleted records: " + deletedRecords.getCount());
        log.info("Enrichment records: " + enrichmentRecords.getCount());
        log.info("Parent relations: " + parentRelationRecords.getCount());
        log.info("Relation errors: " + relationErrors.getCount());
        log.info("Queue errors: " + queueErrors.getCount());
    }

    void commit() throws SQLException {
        connection.commit();
        log.debug("Transcation committed");
    }

    /*
     *
     */
    private static Connection getConnection(String url) throws SQLException {
        Matcher matcher = urlPattern.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(url + " Is not a valid jdbc uri");
        }
        Properties properties = new Properties();
        String jdbc = matcher.group(urlPatternPrefix);
        if (jdbc == null) {
            jdbc = jdbcDefault;
        }
        if (matcher.group(urlPatternUser) != null) {
            properties.setProperty("user", matcher.group(urlPatternUser));
        }
        if (matcher.group(urlPatternPassword) != null) {
            properties.setProperty("password", matcher.group(urlPatternPassword));
        }

        log.debug("Connecting");
        Connection connection = DriverManager.getConnection(jdbc + matcher.group(urlPatternHostPortDb), properties);
        log.debug("Connected");
        return connection;
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;
}
