--
-- fbs-sync wants only modified records
--
INSERT INTO queueworkers (worker) VALUES('fbs-sync');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('opencataloging-update', 'fbs-sync', 'Y', 'A');
--
-- broend-sync wants all searchable records affected by modification
--
INSERT INTO queueworkers (worker) VALUES('broend-sync');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('opencataloging-update', 'broend-sync', 'A', 'Y');
--
-- solr-sync wants only modified records
--
INSERT INTO queueworkers (worker) VALUES('solr-sync');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('opencataloging-update', 'solr-sync', 'Y', 'A');

---
--- rules for bulk loader
---
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('bulk-fbs', 'fbs-sync', 'Y', 'A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('bulk-broend', 'broend-sync', 'A', 'Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES('bulk-solr', 'solr-sync', 'Y', 'A');
