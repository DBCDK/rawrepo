--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 27;
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

CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64),
                                   agencyid_              NUMERIC(6),
                                   provider_              VARCHAR(32),
                                   changed_               CHAR(1),
                                   leaf_                  CHAR(1),
                                   priority_              NUMERIC(4))
  RETURNS SETOF ENQUEUERESULT AS $$ -- V18
DECLARE
  row    QUEUERULES;
  exists QUEUE;
  rows   INT;
  r      ENQUEUERESULT%ROWTYPE;
BEGIN

  FOR row IN SELECT *
             FROM queuerules
             WHERE provider = provider_ AND (changed = 'A' OR changed = changed_) AND (leaf = 'A' OR leaf = leaf_) LOOP
    r.worker = row.worker;
    -- RAISE NOTICE 'worker=%', row.worker;
    SELECT COUNT(*)
    INTO rows
    FROM queue
    WHERE bibliographicrecordid = bibliographicrecordid_ AND agencyid = agencyid_ AND worker = row.worker;
    -- RAISE NOTICE 'rows=%', rows;
    CASE
      WHEN rows = 0
      THEN -- none is queued
        INSERT INTO queue (bibliographicrecordid, agencyid, worker, priority)
        VALUES (bibliographicrecordid_, agencyid_, row.worker, priority_);
        r.queued = TRUE;
        RETURN NEXT r;
        --RETURN QUERY SELECT worker, true;
      WHEN rows = 1
      THEN -- one is queued - but may be locked by a worker
        BEGIN
          SELECT *
          INTO exists
          FROM queue
          WHERE bibliographicrecordid = bibliographicrecordid_ AND agencyid = agencyid_ AND worker = row.worker
          FOR UPDATE NOWAIT;
          -- By locking the row, we ensure that no worker can take this row until we commit / rollback
          -- Ensuring that even if this job is next, it will not be processed until we're sure our data is used.
          UPDATE queue SET priority = priority_
          WHERE bibliographicrecordid = bibliographicrecordid_ AND agencyid = agencyid_ AND worker = row.worker
                AND priority > priority_;
          r.queued = FALSE;
          RETURN NEXT r;
          EXCEPTION
          WHEN lock_not_available
            THEN
              INSERT INTO queue (bibliographicrecordid, agencyid, worker, priority)
              VALUES (bibliographicrecordid_, agencyid_, row.worker, priority_);
              r.queued = TRUE;
              RETURN NEXT r;
        END;
    ELSE
      r.queued = FALSE;
      RETURN NEXT r;
      -- nothing
    END CASE;
  END LOOP;
END
$$
LANGUAGE plpgsql;

DROP TABLE provider_log CASCADE;

COMMIT TRANSACTION;