--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 17;
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

ALTER TABLE jobdiag ALTER COLUMN error TYPE text;

--
--
--
COMMIT;
