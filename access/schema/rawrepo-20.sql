\set ON_ERROR_STOP
-- 
-- dbc-rawrepo-access
-- Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
-- Denmark. CVR: 15149043
--
-- This file is part of dbc-rawrepo-access.
--
-- dbc-rawrepo-access is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- dbc-rawrepo-access is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
-- 

CREATE TABLE version (-- V2
  version NUMERIC(6) NOT NULL PRIMARY KEY,
  warning TEXT DEFAULT NULL -- V7
);
-- Compatible versions
INSERT INTO version VALUES (15);
INSERT INTO version VALUES (16);
INSERT INTO version VALUES (17);
INSERT INTO version VALUES (18);
INSERT INTO version VALUES (19);
INSERT INTO version VALUES (20);

-- records:
-- Primary objective: bibliographicrecordid, agencyid => content(blob)
CREATE TABLE records (-- V2
  bibliographicrecordid VARCHAR(64)              NOT NULL,
  agencyid              NUMERIC(6)               NOT NULL,
  deleted               BOOLEAN                  NOT NULL DEFAULT FALSE, -- V3
  mimetype              VARCHAR(128)             NOT NULL DEFAULT 'text/marcxchange', -- V3
  content               TEXT, -- base64 encoded
  created               TIMESTAMP WITH TIME ZONE NOT NULL,
  modified              TIMESTAMP WITH TIME ZONE NOT NULL,
  trackingId            VARCHAR(256)             NOT NULL DEFAULT '',
  CONSTRAINT records_pk PRIMARY KEY (bibliographicrecordid, agencyid)
);

CREATE UNIQUE INDEX records_relation_id
  ON records (bibliographicrecordid, agencyid, deleted); -- V10
CREATE INDEX records_agencyid
  ON records (agencyid); -- V13

CREATE TABLE records_archive (-- V2
  bibliographicrecordid VARCHAR(64)              NOT NULL,
  agencyid              NUMERIC(6)               NOT NULL,
  deleted               BOOLEAN                  NOT NULL DEFAULT FALSE, -- V3
  mimetype              VARCHAR(128)             NOT NULL DEFAULT 'text/marcxchange', -- V3
  content               TEXT, -- base64 encoded
  created               TIMESTAMP WITH TIME ZONE NOT NULL,
  modified              TIMESTAMP WITH TIME ZONE NOT NULL,
  trackingId            VARCHAR(256)             NOT NULL DEFAULT ''
);

--
-- index for looking up records in archive
CREATE INDEX records_archive_pk
  ON records_archive (bibliographicrecordid, agencyid, modified);
CREATE INDEX records_archive_id
  ON records_archive (bibliographicrecordid, agencyid);
CREATE INDEX records_archive_modified
  ON records_archive (modified);

CREATE OR REPLACE FUNCTION archive_record()
  RETURNS TRIGGER AS $$ -- V12
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION archive_record_cleanup()
  RETURNS TRIGGER AS $$ -- V12
DECLARE
  ts TIMESTAMP WITH TIME ZONE;
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  FOR ts IN
  SELECT modified
  FROM records_archive
  WHERE bibliographicrecordid = OLD.bibliographicrecordid AND agencyid = OLD.agencyid AND
        modified <= NEW.modified - INTERVAL '42 DAYS'
  ORDER BY modified DESC
  OFFSET 1
  LIMIT 1
  LOOP
    DELETE FROM records_archive
    WHERE bibliographicrecordid = OLD.bibliographicrecordid AND agencyid = OLD.agencyid AND modified <= ts;
  END LOOP;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER archive_record_update
  -- V2
AFTER UPDATE ON records
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE archive_record_cleanup();

CREATE TRIGGER archive_record_delete
  -- V2
AFTER DELETE ON records
FOR EACH ROW
EXECUTE PROCEDURE archive_record();

-- relations:
-- bibliographicrecordid, agencyid => refer(bibliographicrecordid, agencyid)
CREATE TABLE relations (-- V2
  bibliographicrecordid       VARCHAR(64) NOT NULL,
  agencyid                    NUMERIC(6)  NOT NULL,
  refer_bibliographicrecordid VARCHAR(64) NOT NULL,
  refer_agencyid              NUMERIC(6)  NOT NULL,
  always_false                BOOLEAN     NOT NULL DEFAULT FALSE, -- V10
  CONSTRAINT relations_pk PRIMARY KEY (bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid),
  CONSTRAINT relations_fk_owner FOREIGN KEY (bibliographicrecordid, agencyid, always_false) REFERENCES records (bibliographicrecordid, agencyid, deleted),
  CONSTRAINT relations_fk_refer FOREIGN KEY (refer_bibliographicrecordid, refer_agencyid, always_false) REFERENCES records (bibliographicrecordid, agencyid, deleted),
  CONSTRAINT relations_no_self_reference CHECK (agencyid <> refer_agencyid OR
                                                bibliographicrecordid <> refer_bibliographicrecordid)
);

--
-- Validate relation to not deleted record
--
CREATE OR REPLACE FUNCTION relation_immutable_false()
  RETURNS TRIGGER AS $$ -- V10
BEGIN
  NEW.always_false = FALSE;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER relation_immutable_false_insert
  -- V10
BEFORE INSERT ON relations
FOR EACH ROW
EXECUTE PROCEDURE relation_immutable_false();

CREATE TRIGGER relation_immutable_false_update
  -- V10
BEFORE UPDATE ON relations
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE relation_immutable_false();

--
-- reverse index for getRelationsChildren()
CREATE INDEX relations_reverse
  ON relations (refer_bibliographicrecordid, refer_agencyid);

--
-- QUEUE complex
--

--
-- List of known workers and attributes to these
--
CREATE TABLE queueworkers (-- V1
  worker VARCHAR(32) NOT NULL, -- name of designated worker
  CONSTRAINT queueworkers_pk PRIMARY KEY (worker)
);


CREATE TABLE queue (-- V2
  bibliographicrecordid VARCHAR(64)              NOT NULL,
  agencyid              NUMERIC(6)               NOT NULL,
  worker                VARCHAR(32)              NOT NULL, -- name of designated worker
  queued                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timeofday() :: TIMESTAMP, -- timestamp for when it has been put into the queue
  CONSTRAINT queue_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker)
  -- NO primary key
  -- if it's claimed by worker
  -- a new job should be reinserted
);


CREATE TABLE messagequeuerules (
  worker    VARCHAR(32) NOT NULL, -- name of designated worker
  queuename VARCHAR(32) NOT NULL, -- name of queue
  CONSTRAINT messagequeuerules_pk PRIMARY KEY (worker),
  CONSTRAINT messagequeuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker),
  CONSTRAINT messagequeuerules_uniq_queuename UNIQUE (queuename)
);


CREATE TABLE jobdiag (-- V17
  bibliographicrecordid VARCHAR(64)              NOT NULL,
  agencyid              NUMERIC(6)               NOT NULL,
  worker                VARCHAR(32)              NOT NULL, -- name of designated worker
  error                 TEXT                     NOT NULL, -- errormessage
  queued                TIMESTAMP WITH TIME ZONE NOT NULL     -- timestamp for when it has been put into the queue
  -- NO primary key
  -- if it's claimed by worker
  -- a new job should be reinserted
);

--
-- Rules to tell which workses should get which types of events
-- When a given provider queues a job
--
CREATE TABLE queuerules (-- V18
  provider VARCHAR(32) NOT NULL, -- name of worker adding data
  worker   VARCHAR(32) NOT NULL, -- name of designated worker
  changed  CHAR(1)     NOT NULL, -- queue jobs if changes Y(es), N(no), A(ll)
  leaf     CHAR(1)     NOT NULL, -- queue jobs if leaf    Y(es), N(no), A(ll)
  -- changed AND leaf should be true to queue
  CONSTRAINT queuerules_pk PRIMARY KEY (provider, worker, changed, leaf),
  CONSTRAINT queuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker)
);


CREATE INDEX queue_idx_job
  ON queue (bibliographicrecordid, agencyid, worker);
CREATE INDEX queue_idx_worker
  ON queue (worker, queued); --V4
CREATE INDEX jobdiag_idx
  ON jobdiag (worker, error, queued); --V7
-- DROP TYPE enqueueResult;
CREATE TYPE ENQUEUERESULT AS (worker VARCHAR(32), queued BOOLEAN);

CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), provider_ VARCHAR(32),
                                   changed_               CHAR(1), leaf_ CHAR(1))
  RETURNS SETOF ENQUEUERESULT AS $$ -- V18
DECLARE
  row    QUEUERULES;
  exists QUEUE;
  rows   INT;
  r      enqueueResult%ROWTYPE;
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
        INSERT INTO queue (bibliographicrecordid, agencyid, worker)
        VALUES (bibliographicrecordid_, agencyid_, row.worker);
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
              INSERT INTO queue (bibliographicrecordid, agencyid, worker)
              VALUES (bibliographicrecordid_, agencyid_, row.worker);
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


CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128),
                                   provider_              VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1))
  RETURNS SETOF VARCHAR(32) AS $$ -- V3,V8
DECLARE
  row    QUEUERULES;
  exists QUEUE;
  rows   INT;
BEGIN
  FOR row IN SELECT *
             FROM queuerules
             WHERE provider = provider_ AND (changed = 'A' OR changed = changed_) AND (leaf = 'A' OR leaf = leaf_) LOOP
    -- RAISE NOTICE 'worker=%', row.worker;
    SELECT COUNT(*)
    INTO rows
    FROM queue
    WHERE bibliographicrecordid = bibliographicrecordid_ AND agencyid = agencyid_ AND worker = row.worker;
    -- RAISE NOTICE 'rows=%', rows;
    CASE
      WHEN rows = 0
      THEN -- none is queued
        INSERT INTO queue (bibliographicrecordid, agencyid, worker)
        VALUES (bibliographicrecordid_, agencyid_, row.worker);
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
          EXCEPTION
          WHEN lock_not_available
            THEN
              INSERT INTO queue (bibliographicrecordid, agencyid, worker)
              VALUES (bibliographicrecordid_, agencyid_, row.worker);
        END;
    ELSE
    -- nothing
    END CASE;
  END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128))
  RETURNS SETOF QUEUE AS $$ -- V8
DECLARE
  row QUEUE;
  upd QUEUE;
BEGIN
  << done >>
    -- LIMIT xxx CUT OFF TO LARGE DATA SETS, SHOULD BE > MAX WORKERTHREADS
  FOR row IN SELECT *
             FROM queue
             WHERE worker = worker_
             ORDER BY queued
             LIMIT 256 LOOP
    BEGIN
      -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
      -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
      -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
      FOR upd IN SELECT *
                 FROM queue
                 WHERE
                   bibliographicrecordid = row.bibliographicrecordid AND agencyid = row.agencyid AND worker = worker_
                 FOR UPDATE NOWAIT LOOP
        DELETE FROM queue
        WHERE bibliographicrecordid = row.bibliographicrecordid AND agencyid = row.agencyid AND worker = worker_;
        RETURN NEXT row;
        EXIT done; -- We got one - exit
      END LOOP;
      EXCEPTION
      WHEN lock_not_available
        THEN
    END;
  END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128), no_ INT)
  RETURNS SETOF QUEUE AS $$ -- V8
DECLARE
  row QUEUE;
  upd QUEUE;
  no  INT = 0;
BEGIN
  << done >>
    -- LIMIT xxx CUT OFF TO LARGE DATA SETS, SHOULD BE > MAX WORKERTHREADS
  FOR row IN SELECT *
             FROM queue
             WHERE worker = worker_
             ORDER BY queued LOOP
    BEGIN
      -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
      -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
      -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
      FOR upd IN SELECT *
                 FROM queue
                 WHERE
                   bibliographicrecordid = row.bibliographicrecordid AND agencyid = row.agencyid AND worker = worker_
                 FOR UPDATE NOWAIT LOOP
        DELETE FROM queue
        WHERE bibliographicrecordid = row.bibliographicrecordid AND agencyid = row.agencyid AND worker = worker_;
        RETURN NEXT row;
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


CREATE OR REPLACE FUNCTION queues(provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1))
  RETURNS SETOF VARCHAR(32) AS $$
BEGIN
  RETURN QUERY SELECT queuename
               FROM queuerules
                 JOIN messagequeuerules USING (worker)
               WHERE provider = provider_ AND (changed = 'A' OR changed = changed_) AND (leaf = 'A' OR leaf = leaf_);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION messagequeuenames()
  RETURNS VOID AS $$
BEGIN
  DELETE FROM messagequeuerules;
  INSERT INTO messagequeuerules (worker, queuename) SELECT
                                                      worker,
                                                      SUBSTR(LOWER(worker), 1, 1) ||
                                                      SUBSTR(REPLACE(INITCAP(worker), '-', ''), 2)
                                                    FROM queueworkers;
END
$$ LANGUAGE plpgsql;
