/*
 * dbc-rawrepo-solr-indexer
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-solr-indexer.
 *
 * dbc-rawrepo-solr-indexer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-solr-indexer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-solr-indexer.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.indexer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import dk.dbc.eeconfig.EEConfig;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.*;
import dk.dbc.rawrepo.exception.SolrIndexerRawRepoException;
import dk.dbc.rawrepo.exception.SolrIndexerSolrException;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 */
@Stateless
public class Indexer {

    private final static Logger log = LoggerFactory.getLogger(Indexer.class);

    private final static String TRACKING_ID = "trackingId";

    @Inject
    @EEConfig.Name(C.SOLR_URL)
    @NotNull
    String solrUrl;

    @Inject
    @EEConfig.Name(C.OPENAGENCY_URL)
    @NotNull
    String openAgencyUrl;

    @Resource(lookup = C.DATASOURCE)
    DataSource dataSource;

    @Inject
    @EEConfig.Name(C.WORKER_NAME)
    @EEConfig.Default(C.WORKER_NAME_DEFAULT)
    @NotNull
    String workerName;

    @Inject
    MetricsRegistry registry;

    @Inject
    MergerPool mergerPool;

    Timer processJobTimer;
    Timer getConnectionTimer;
    Timer createDAOTimer;
    Timer dequeueJobTimer;
    Timer fetchRecordTimer;
    Timer createIndexDocumentTimer;
    Timer updateSolrTimer;
    Timer deleteSolrDocumentTimer;
    Timer queueFailTimer;
    Timer commitTimer;

    Counter contentsIndexed;
    Counter contentsSkipped;
    Counter contentsFailed;

    JavaScriptWorker worker;

    private SolrServer solrServer;

    private OpenAgencyServiceFromURL openAgency;

    public Indexer() {
        this.solrServer = null;
    }

    @PostConstruct
    public void create() {
        // Read solr url from application context
        log.info("Initializing with url {}", solrUrl);
        solrServer = new HttpSolrServer(solrUrl);
        openAgency = OpenAgencyServiceFromURL.builder().build(openAgencyUrl);

        processJobTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "processJob"));
        getConnectionTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "getConnection"));
        createDAOTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "createDAO"));
        dequeueJobTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "dequeueJob"));
        fetchRecordTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "fetchRecord"));
        createIndexDocumentTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "createIndexDocument"));
        updateSolrTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "updateSolr"));
        deleteSolrDocumentTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "deleteSolrDocument"));
        queueFailTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "queueFail"));
        commitTimer = registry.getRegistry().timer(MetricRegistry.name(Indexer.class, "commit"));
        contentsIndexed = registry.getRegistry().counter(MetricRegistry.name(Indexer.class, "contentsIndexed"));
        contentsSkipped = registry.getRegistry().counter(MetricRegistry.name(Indexer.class, "contentsSkipped"));
        contentsFailed = registry.getRegistry().counter(MetricRegistry.name(Indexer.class, "contentsFailed"));

        worker = new JavaScriptWorker();
    }

    @PreDestroy
    public void destroy() {
        solrServer.shutdown();
    }

    public void performWork() throws SolrIndexerRawRepoException, SolrIndexerSolrException {

        boolean moreWork = true;
        int processedJobs = 0;

        // Test connection and return proper error if there is a problem
        try {
            solrServer.ping();
        } catch (SolrServerException | IOException | HttpSolrServer.RemoteSolrException ex) {
            throw new SolrIndexerSolrException("Could not connect to the solr server. " + ex.getMessage(), ex);
        }

        while (moreWork) {
            Timer.Context time = processJobTimer.time();
            try (Connection connection = getConnection()) {
                RawRepoDAO dao = createDAO(connection);
                try {
                    QueueJob job = dequeueJob(dao);

                    if (job != null) {
                        MDC.put(TRACKING_ID, createTrackingId(job));
                        processJob(job, dao);
                        commit(connection);
                        processedJobs++;
                        if (processedJobs % 1000 == 0) {
                            log.info("Still indexing {} jobs from '{}'", processedJobs, workerName);
                        }
                        time.stop();
                    } else {
                        moreWork = false;
                        log.trace("Queue is empty. Nothing to index");
                    }
                } catch (RawRepoException | IllegalArgumentException | MarcXMergerException | SQLException ex) {
                    connection.rollback();
                    throw ex;
                }
            } catch (SQLException ex) {
                // If we get a SQLException there is something wrong which we can't do anything about.
                log.error("SQLException: ", ex);
                throw new SolrIndexerRawRepoException("SQL exception from rawrepo:" + ex.toString(), ex);
            } catch (MarcXMergerException | RawRepoException | RuntimeException ex) {
                log.error("Error getting job from database", ex);
                moreWork = false;
            } finally {
                MDC.remove(TRACKING_ID);
            }
        }
        if (processedJobs > 0) {
            log.info("Done indexing {} jobs from '{}'", processedJobs, workerName);
        }
    }

    protected Connection getConnection() throws SQLException {
        Timer.Context time = getConnectionTimer.time();
        final Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        time.stop();
        return connection;
    }

    private RawRepoDAO createDAO(final Connection connection) throws RawRepoException {
        try (Timer.Context time = createDAOTimer.time()) {
            return RawRepoDAO.builder(connection).relationHints(new RelationHintsOpenAgency(openAgency)).build();
        }
    }

    private void processJob(QueueJob job, RawRepoDAO dao) throws RawRepoException, MarcXMergerException, SolrIndexerSolrException {
        log.info("Indexing {}", job);
        RecordId jobId = job.getJob();
        String id = jobId.getBibliographicRecordId();
        int library = jobId.getAgencyId();
        try {
            Record record = fetchRecord(dao, id, library);
            if (record == null) {
                log.info("record from {} does not exist, most likely queued by dependency", job);
                return;
            }
            MDC.put(TRACKING_ID, createTrackingId(job, record));
            if (record.isDeleted()) {
                deleteSolrDocument(jobId);
            } else {
                SolrInputDocument doc = createIndexDocument(record);
                updateSolr(jobId, doc);
            }
            log.info("Indexed {}", job);
        } catch (RawRepoExceptionRecordNotFound ex) {
            log.error("Queued record does not exist {}", job);
            queueFail(dao, job, ex.getMessage());
        } catch (HttpSolrServer.RemoteSolrException ex) {
            // Index is missing on the solr server so we need to stop now
            if (ex.getMessage().contains("unknown field")) {
                throw new SolrIndexerSolrException("Missing index: " + ex.getMessage(), ex);
            }
            log.error("Error processing {}", job, ex);
            queueFail(dao, job, ex.getMessage());
        } catch (RawRepoException | SolrException | SolrServerException | IOException ex) {
            log.error("Error processing {}", job, ex);
            queueFail(dao, job, ex.getMessage());
        }
    }

    private QueueJob dequeueJob(final RawRepoDAO dao) throws RawRepoException {
        Timer.Context time = dequeueJobTimer.time();
        final QueueJob job = dao.dequeue(workerName);
        if (job != null) {
            time.stop();
        }
        return job;
    }

    private Record fetchRecord(RawRepoDAO dao, String id, int library) throws RawRepoException, MarcXMergerException {
        MarcXMerger merger = null;
        try (Timer.Context time = fetchRecordTimer.time()) {
            if (!dao.recordExistsMaybeDeleted(id, library)) {
                return null;
            }
            merger = mergerPool.getMerger();
            return dao.fetchMergedRecordExpanded(id, library, merger, true);
        } finally {
            if (merger != null) {
                mergerPool.putMerger(merger);
            }
        }
    }

    private String createSolrDocumentId(RecordId recordId) {
        return recordId.getBibliographicRecordId() + ":" + recordId.getAgencyId();
    }

    SolrInputDocument createIndexDocument(Record record) {
        Timer.Context time = createIndexDocumentTimer.time();
        final SolrInputDocument doc = new SolrInputDocument();
        RecordId recordId = record.getId();
        doc.addField("id", createSolrDocumentId(recordId));

        String mimeType = record.getMimeType();
        switch (mimeType) {
            case MarcXChangeMimeType.MARCXCHANGE:
            case MarcXChangeMimeType.ARTICLE:
            case MarcXChangeMimeType.AUTHORITY:
            case MarcXChangeMimeType.ENRICHMENT:
                log.debug("Indexing content of {} with mimetype {}", recordId, mimeType);
                String content = new String(record.getContent(), StandardCharsets.UTF_8);
                try {
                    worker.addFields(doc, content, mimeType);
                    contentsIndexed.inc();
                } catch (Exception ex) {
                    log.error("Error adding fields for document '{}': ", content, ex);
                    contentsFailed.inc();
                }
                break;
            default:
                contentsSkipped.inc();
                log.debug("Skipping indexing of {} with mimetype {}", recordId, mimeType);
        }

        doc.addField("rec.bibliographicRecordId", recordId.getBibliographicRecordId());
        doc.addField("rec.agencyId", recordId.getAgencyId());
        doc.addField("rec.created", record.getCreated());
        doc.addField("rec.modified", record.getModified());
        doc.addField("rec.trackingId", record.getTrackingId());
        log.trace("Created solr document {}", doc);
        time.stop();
        return doc;
    }

    private void deleteSolrDocument(RecordId jobId) throws IOException, SolrServerException {
        Timer.Context time = deleteSolrDocumentTimer.time();
        log.debug("Deleting document for {} to solr", jobId);
        solrServer.deleteById(createSolrDocumentId(jobId));
        time.stop();
    }

    private void updateSolr(RecordId jobId, SolrInputDocument doc) throws IOException, SolrServerException {
        Timer.Context time = updateSolrTimer.time();
        log.debug("Adding document for {} to solr", jobId);
        solrServer.add(doc);
        time.stop();
    }

    private void queueFail(RawRepoDAO dao, QueueJob job, String error) throws RawRepoException {
        Timer.Context time = queueFailTimer.time();
        dao.queueFail(job, error);
        time.stop();
    }

    private void commit(final Connection connection) throws SQLException {
        Timer.Context time = commitTimer.time();
        connection.commit();
        time.stop();
    }

    private static String createTrackingId(QueueJob job) {
        return "RawRepoIndexer:" + job.toString();
    }

    private static String createTrackingId(QueueJob job, Record record) {
        String trackingId = record.getTrackingId();
        if (trackingId == null || trackingId.isEmpty()) {
            return createTrackingId(job);
        } else {
            return createTrackingId(job) + "<" + trackingId;
        }
    }
}
