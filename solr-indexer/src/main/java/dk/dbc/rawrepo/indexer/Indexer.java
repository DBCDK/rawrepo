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
package dk.dbc.rawrepo.indexer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.AgencySearchOrder;
import dk.dbc.rawrepo.AgencySearchOrderFallback;
import dk.dbc.rawrepo.RawRepoException;
import java.nio.charset.StandardCharsets;

/**
 *
 */
@Stateless
public class Indexer {

    private final static Logger log = LoggerFactory.getLogger(Indexer.class);

    @Resource(name = "solrUrl")
    String solrUrl;

    @Resource(lookup = "jdbc/rawrepoindexer/rawrepo")
    DataSource dataSource;

    @Resource(name = "workerName")
    String workerName;

    @Inject
    MetricsRegistry registry;

    @Inject
    MergerPool mergerPool;

    AgencySearchOrder searchOrder;

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

    public Indexer() {
    }

    @PostConstruct
    public void create() {
        // Read solr url from application context
        log.info("Initializing with url {}", solrUrl);
        solrServer = new HttpSolrServer(solrUrl);

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
        searchOrder = new AgencySearchOrderFallback();
    }

    @PreDestroy
    public void destroy() {
        solrServer.shutdown();
    }

    public void performWork() {
        log.info("Indexing available jobs from worker '{}'", workerName);

        boolean moreWork = true;
        int processedJobs = 0;

        while (moreWork) {

            Timer.Context time = processJobTimer.time();
            try (Connection connection = getConnection()) {
                RawRepoDAO dao = createDAO(connection);
                QueueJob job = dequeueJob(dao);

                if (job != null) {
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
            } catch (MarcXMergerException | RawRepoException | SQLException ex) {
                moreWork = false;
                log.error("Error getting job from database", ex);
            }
        }
        log.info("Done indexing {} jobs from '{}'", processedJobs, workerName);
    }

    protected Connection getConnection() throws SQLException {
        Timer.Context time = getConnectionTimer.time();
        final Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        time.stop();
        return connection;
    }

    private RawRepoDAO createDAO(final Connection connection) throws RawRepoException {
        Timer.Context time = createDAOTimer.time();
        final RawRepoDAO dao = RawRepoDAO.newInstance(connection, searchOrder);
        time.stop();
        return dao;
    }

    private void processJob(QueueJob job, RawRepoDAO dao) throws RawRepoException, MarcXMergerException {
        log.debug("Indexing {}", job);
        RecordId jobId = job.getJob();
        String id = jobId.getBibliographicRecordId();
        int library = jobId.getAgencyId();
        try {
            Record record = fetchRecord(dao, id, library);
            if (record.isDeleted()) {
                deleteSolrDocument(jobId);
            } else {
                SolrInputDocument doc = createIndexDocument(record);
                updateSolr(jobId, doc);
            }
        } catch (RawRepoException | SolrException | SolrServerException | IOException ex) {
            log.error("Error processing {}", job, ex);
            queueFail(dao, job, id);
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
            merger = mergerPool.getMerger();
            return dao.fetchMergedRecord(id, library, merger, true);
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
            case MarcXChangeMimeType.AUTHORITTY:
            case MarcXChangeMimeType.ENRICHMENT:
                log.debug("Indexing content of {} with mimetype {}", recordId, mimeType);
                doc.addField("marc.001a", recordId.getBibliographicRecordId());
                doc.addField("marc.001b", recordId.getAgencyId());
                byte[] content = record.getContent();
                try {
                    worker.addFields(doc, new String(content, StandardCharsets.UTF_8), mimeType);
                    contentsIndexed.inc();
                } catch (Exception ex) {
                    log.error("Error adding fields: ", ex);
                    contentsFailed.inc();
                }
                break;
            default:
                contentsSkipped.inc();
                log.debug("Skipping indexing of {} with mimetype {}", recordId, mimeType);
        }

        doc.addField("created", record.getCreated());
        doc.addField("modified", record.getModified());
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

    private void queueFail(RawRepoDAO dao, QueueJob job, String id) throws RawRepoException {
        Timer.Context time = queueFailTimer.time();
        dao.queueFail(job, id);
        time.stop();
    }

    private void commit(final Connection connection) throws SQLException {
        Timer.Context time = commitTimer.time();
        connection.commit();
        time.stop();
    }
}
