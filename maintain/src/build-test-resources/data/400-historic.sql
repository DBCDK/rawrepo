INSERT INTO records(bibliographicrecordid, agencyid, content, created, modified)
       VALUES('H1', 100000, encode('<rec incarnation="1"/>', 'base64'), '2010-01-01 00:00:00', '2015-01-01 00:00:00');


UPDATE records SET content=encode('<rec incarnation="2"/>', 'base64'), modified='2015-01-01 01:00:00' WHERE bibliographicrecordid='H1' AND agencyid=100000;
UPDATE records SET content=encode('<rec incarnation="3"/>', 'base64'), modified='2015-02-01 00:00:00' WHERE bibliographicrecordid='H1' AND agencyid=100000;

INSERT INTO records(bibliographicrecordid, agencyid, content, created, modified)
       VALUES('H2', 100000, encode('<rec incarnation="1"/>', 'base64'), '2010-01-01 00:00:00', '2015-01-01 00:00:00');
