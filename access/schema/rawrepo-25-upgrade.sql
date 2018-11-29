--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 25;
  brokenversion  INTEGER = 20;
  OLDversion     INTEGER;
BEGIN
  SELECT MAX(version)
  INTO OLDversion
  FROM version;
  IF (OLDversion <> (currentversion - 1))
  THEN
    RAISE EXCEPTION 'Expected schema version % found %', (currentversion - 1), OLDversion;
  END IF;
  INSERT INTO version VALUES (currentversion);
  DELETE FROM version
  WHERE version = brokenversion;
END
$$;

--
--
--

DELETE FROM records_cache;
ALTER TABLE records_cache ADD COLUMN enrichmenttrail TEXT;

COMMIT TRANSACTION;