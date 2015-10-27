--
-- broend-sync wants all searchable records affected by modification
--
INSERT INTO queueworkers (worker) VALUES('broend-sync');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('opencataloging-update', 'broend-sync', '', 'A', 'Y');
--
-- solr-sync wants only modified records
--
INSERT INTO queueworkers (worker) VALUES('solr-sync');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('opencataloging-update', 'solr-sync', '', 'Y', 'A');

--
--
--
INSERT INTO queueworkers (worker) VALUES('basis-decentral');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('opencataloging-update', 'basis-decentral', '', 'Y', 'A');

INSERT INTO queueworkers (worker) VALUES('solr-sync-bulk');

---
--- rules for bulk loader
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('bulk-broend', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('bulk-solr', 'solr-sync-bulk', '', 'Y', 'A');

---
--- rules for agency-delete
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'solr-sync', '', 'Y', 'A');

---
--- rules for agency-maintain
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-maintain', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-maintain', 'solr-sync', '', 'Y', 'A');

---
--- rules for fbs-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'solr-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'basis-decentral', '', 'Y', 'A');

---
--- rules for dataio-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'solr-sync', '', 'Y', 'A');
