--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 9;
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

CREATE OR REPLACE FUNCTION record_delete_cascade() RETURNS TRIGGER AS $$ -- V9
BEGIN
    DELETE FROM relations WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid;
    DELETE FROM relations WHERE refer_bibliographicrecordid=OLD.bibliographicrecordid AND refer_agencyid=OLD.agencyid;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER record_delete_cascade -- V9
    AFTER DELETE ON records
    FOR EACH ROW
    EXECUTE PROCEDURE record_delete_cascade();


--
--
--
COMMIT;
