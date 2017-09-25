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

CREATE OR REPLACE FUNCTION enqueue_bulk(bibliographicrecordid_ character varying[], agencyid_ numeric[], provider_ character varying[], changed_ character varying[], leaf_ character varying[]) RETURNS TABLE(bibliographicrecordid character varying, agencyid numeric, worker character varying, queued boolean)
    LANGUAGE plpgsql
    AS $$ -- V21
DECLARE
  elements_max     INTEGER := array_length(bibliographicrecordid_, 1);
  elements_current INTEGER := 1;
BEGIN
  WHILE elements_current <= elements_max LOOP
    FOR worker, queued IN
    SELECT
      e.worker,
      e.queued
    FROM enqueue(bibliographicrecordid_ [elements_current],
                 agencyid_ [elements_current],
                 provider_ [elements_current],
                 changed_ [elements_current],
                 leaf_ [elements_current]) AS e
    LOOP
      bibliographicrecordid = bibliographicrecordid_ [elements_current];
      agencyid = agencyid_ [elements_current];
      RETURN NEXT;
    END LOOP;

    elements_current = elements_current + 1;
  END LOOP;
END
$$;

--
--
--
COMMIT;
