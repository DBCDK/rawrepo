--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 12;
  brokenversion INTEGER = 0;
  oldversion INTEGER;
BEGIN
  SELECT MAX(version) INTO oldversion FROM version;
  IF (oldversion <> (currentversion-1)) THEN
    RAISE EXCEPTION 'Expected schema version % found %', (currentversion-1), oldversion;
  END IF;
  INSERT INTO version VALUES(currentversion);
  DELETE FROM version WHERE version <= brokenversion;
END
$$;


--
--
--

ALTER TABLE records ADD COLUMN trackingid character varying(256) NOT NULL DEFAULT '';
ALTER TABLE records_archive ADD COLUMN trackingid character varying(256) NOT NULL DEFAULT '';


CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V12
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified, OLD.trackingId);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND
                                                   modified <=  NOW() - INTERVAL '42 DAYS' ORDER BY modified DESC OFFSET 1 LIMIT 1
    LOOP
	DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;


--
--
--
COMMIT;
