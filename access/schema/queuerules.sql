--
-- Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
--  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
--
INSERT INTO queueworkers (worker) VALUES ('basis-decentral');
INSERT INTO queueworkers (worker) VALUES ('broend-sync');
INSERT INTO queueworkers (worker) VALUES ('danbib-ph-libv3');
INSERT INTO queueworkers (worker) VALUES ('dataio-bulk-sync');
INSERT INTO queueworkers (worker) VALUES ('dataio-socl-sync-bulk');
INSERT INTO queueworkers (worker) VALUES ('ims-bulk-sync');
INSERT INTO queueworkers (worker) VALUES ('ims-sync');
INSERT INTO queueworkers (worker) VALUES ('oai-set-matcher');
INSERT INTO queueworkers (worker) VALUES ('socl-sync');
INSERT INTO queueworkers (worker) VALUES ('solr-sync-bulk');

INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('agency-delete','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('agency-delete','socl-sync','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('agency-maintain','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('agency-maintain','socl-sync','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('bulk-broend','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-bulk','dataio-bulk-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-bulk','dataio-socl-sync-bulk','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-bulk','oai-set-matcher','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-ph-holding-update','danbib-ph-libv3','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update','oai-set-matcher','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update','socl-sync','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update-well3.5','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update-well3.5','ims-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('dataio-update-well3.5','socl-sync','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-ph-update','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-ph-update','danbib-ph-libv3','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-ph-update', 'socl-sync', 'Y', 'A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-update','basis-decentral','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-update','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-update','ims-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('fbs-update', 'socl-sync', 'Y', 'A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('ims','ims-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('ims-bulk','ims-bulk-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('solr-sync-bulk', 'solr-sync-bulk', 'Y', 'N');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('opencataloging-update','basis-decentral','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('opencataloging-update','broend-sync','A','Y');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('opencataloging-update','socl-sync','Y','A');
INSERT INTO queuerules (provider, worker, changed, leaf) VALUES ('update-rawrepo-solr-sync','socl-sync','Y','N');