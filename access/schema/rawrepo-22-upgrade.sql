--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 22;
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

-- Queue table

DROP INDEX queue_idx_worker;

ALTER TABLE queue
  ADD COLUMN
  priority NUMERIC(4) DEFAULT 1000 NOT NULL;

CREATE INDEX queue_idx_worker
  ON queue (worker, priority, queued); --V22

-- Jobdiag table

DROP INDEX jobdiag_idx;

ALTER TABLE jobdiag
  ADD COLUMN priority NUMERIC(4) DEFAULT 1000 NOT NULL;

CREATE INDEX jobdiag_idx
  ON jobdiag (worker, error, queued, priority); --V22

-- Enqueue functions

CREATE TABLE provider_log (
  provider  VARCHAR(32) NOT NULL,
  hit_count NUMERIC     NOT NULL DEFAULT 1,
  modified  TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX provider_log_idx
  ON provider_log (provider);

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

  INSERT INTO provider_log (provider, hit_count, modified) VALUES (provider_, 1, now())
  ON CONFLICT (provider)
    DO UPDATE SET hit_count = provider_log.hit_count + 1, modified = now();

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
$$ LANGUAGE plpgsql;

--- DEPRECATED as of December 2017
CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128),
                                   provider_              VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1))
  RETURNS SETOF VARCHAR(32) AS $$ -- V3, V8, V22
BEGIN
  SELECT *
  FROM enqueue(bibliographicrecordid_, agencyid_, provider_, changed_, leaf_, 1000);
END
$$ LANGUAGE plpgsql;


--- DEPRECATED as of December 2017
CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64),
                                   agencyid_              NUMERIC(6),
                                   provider_              VARCHAR(32),
                                   changed_               CHAR(1),
                                   leaf_                  CHAR(1))
  RETURNS SETOF ENQUEUERESULT AS $$ -- V18, V22
BEGIN
  RETURN QUERY
  SELECT *
  FROM enqueue(bibliographicrecordid_, agencyid_, provider_, changed_, leaf_, 1000);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION enqueue_bulk(bibliographicrecordid_ VARCHAR(64) [],
                                        agencyid_              NUMERIC(6) [],
                                        provider_              VARCHAR(32) [],
                                        changed_               VARCHAR(1) [],
                                        leaf_                  VARCHAR(1) [])
  RETURNS TABLE(bibliographicrecordid VARCHAR(64), agencyid NUMERIC(6), worker VARCHAR(32), queued BOOLEAN) AS $$ -- V21
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
                 leaf_ [elements_current],
                 1000) AS e -- When bulk enqueuing we always want to use default priority
    LOOP
      bibliographicrecordid = bibliographicrecordid_ [elements_current];
      agencyid = agencyid_ [elements_current];
      RETURN NEXT;
    END LOOP;

    elements_current = elements_current + 1;
  END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128))
  RETURNS SETOF QUEUE AS $$ -- V8
BEGIN
  RETURN QUERY
  SELECT *
  FROM dequeue(worker_, 1);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128), no_ INT)
  RETURNS SETOF QUEUE AS $$ -- V8
DECLARE
  queue_row        QUEUE;
  queue_update_row QUEUE;
  no               INT = 0;
BEGIN
  << done >>
    -- LIMIT xxx CUT OFF TO LARGE DATA SETS, SHOULD BE > MAX WORKERTHREADS
  FOR queue_row IN SELECT *
                   FROM queue
                   WHERE worker = worker_
                   ORDER BY priority, queued LOOP
    BEGIN
      -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
      -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
      -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
      FOR queue_update_row IN SELECT *
                              FROM queue
                              WHERE
                                bibliographicrecordid = queue_row.bibliographicrecordid AND
                                agencyid = queue_row.agencyid AND worker = worker_
                              FOR UPDATE NOWAIT LOOP
        DELETE FROM queue
        WHERE bibliographicrecordid = queue_row.bibliographicrecordid AND agencyid = queue_row.agencyid AND
              worker = worker_;
        RETURN NEXT queue_row;
        no = no + 1;
        IF no >= no_
        THEN
          EXIT done; -- We got one - exit
        END IF;
      END LOOP;
      EXCEPTION
      WHEN lock_not_available
        THEN
    END;
  END LOOP;
END
$$ LANGUAGE plpgsql;

--
--
--
COMMIT;
