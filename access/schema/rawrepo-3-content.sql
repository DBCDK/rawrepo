--
--
--
INSERT INTO queueworkers (worker) VALUES('basis-decentral');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('opencataloging-update', 'basis-decentral', 'text/decentral+marcxchange', 'Y', 'A');

UPDATE records SET mimetype='text/enrichment+marcxchange' WHERE EXISTS (SELECT * FROM relations WHERE agencyid=records.agencyid AND bibliographicrecordid=records.bibliographicrecordid AND refer_bibliographicrecordid=records.bibliographicrecordid);
