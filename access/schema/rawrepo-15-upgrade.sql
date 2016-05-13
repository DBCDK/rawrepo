--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 15;
  brokenversion INTEGER = 14;
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


--
--
--
COMMIT;

DO $$ BEGIN
RAISE NOTICE '';
RAISE NOTICE ' UPGRADE CONTENT SWAP 191919 <-> 870970 ';
RAISE NOTICE '';
END $$;

