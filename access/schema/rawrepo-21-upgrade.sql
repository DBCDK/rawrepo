--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 21;
  brokenversion  INTEGER = 20;
  oldversion     INTEGER;
BEGIN
  SELECT MAX(version)
  INTO oldversion
  FROM version;
  IF (oldversion <> (currentversion - 1))
  THEN
    RAISE EXCEPTION 'Expected schema version % found %', (currentversion - 1), oldversion;
  END IF;
  INSERT INTO version VALUES (currentversion);
  DELETE FROM version
  WHERE version = brokenversion;
END
$$;

--
--
--

DROP FUNCTION queues(provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1) );
DROP FUNCTION messagequeuenames();
DROP TABLE messagequeuerules;

--
--
--
COMMIT;
