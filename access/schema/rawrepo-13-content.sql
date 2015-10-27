
--- Drop fbs-sync


DELETE FROM queuerules WHERE worker='fbs-sync';
DELETE FROM queueworkers WHERE worker='fbs-sync';

INSERT INTO queueworkers VALUES ('solr-sync-bulk');
UPDATE queuerules SET worker='solr-sync-bulk' WHERE provider='bulk-solr' AND worker='solr-sync';

INSERT INTO queuerules VALUES ('agency-maintain', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules VALUES ('agency-maintain', 'solr-sync', '', 'Y', 'A');
