
---
--- rules for agency-delete
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'solr-sync', '', 'Y', 'A');


---
--- rules for fbs-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'solr-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'basis-decentral', '', 'Y', 'A');

---
--- rules for dataio-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'solr-sync', '', 'Y', 'A');
