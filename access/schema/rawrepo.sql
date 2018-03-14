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
INSERT INTO version VALUES (21);
INSERT INTO version VALUES (22);
INSERT INTO version VALUES (23);

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

CREATE OR REPLACE FUNCTION update_records_archive()
  RETURNS TRIGGER AS $$ -- V23
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_records_archive()
  RETURNS TRIGGER AS $$ -- V23
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER records_update_trig_archive
  -- V23
  AFTER UPDATE
  ON records
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE update_records_archive();

CREATE TRIGGER records_delete_trig_archive
  -- V23
  AFTER DELETE
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE delete_records_archive();

--
-- records_summary table plus triggers
--
CREATE TABLE records_summary (-- V23
  agencyid         NUMERIC(6) PRIMARY KEY   NOT NULL,
  original_count   NUMERIC                  NOT NULL DEFAULT 0,
  enrichment_count NUMERIC                  NOT NULL DEFAULT 0,
  deleted_count    NUMERIC                  NOT NULL DEFAULT 0,
  ajour_date       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION insert_records_summary()
  RETURNS TRIGGER AS $$ -- V23
DECLARE
  _original_count   NUMERIC := 0;
  _enrichment_count NUMERIC := 0;
  _deleted_count    NUMERIC := 0;
  _summary_count    NUMERIC;
BEGIN
  -- Since this is an insert it might be the first record for that agency, so we can't be sure there is a row for that
  -- agency in records_summary
  SELECT count(*)
  INTO _summary_count
  FROM records_summary
  WHERE records_summary.agencyid = NEW.agencyid;

  IF _summary_count > 0
  THEN
    SELECT
      rs.original_count,
      rs.enrichment_count,
      rs.deleted_count
    INTO _original_count, _enrichment_count, _deleted_count
    FROM records_summary rs
    WHERE rs.agencyid = NEW.agencyid
    FOR UPDATE;
  END IF;

  IF NEW.deleted
  THEN -- Handled deleted record (this probably won't happen, but just to be sure...)
    _deleted_count := _deleted_count + 1;
  ELSEIF NEW.mimetype = 'text/enrichment+marcxchange'
    THEN -- Handle enrichment
      _enrichment_count := _enrichment_count + 1;
  ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
    THEN -- Handle original record
      _original_count := _original_count + 1;
  END IF;

  INSERT INTO records_summary (agencyid, original_count, enrichment_count, deleted_count, ajour_date)
  VALUES (NEW.agencyid, _original_count, _enrichment_count, _deleted_count, now())
  ON CONFLICT (agencyid)
    DO UPDATE SET original_count = _original_count,
      enrichment_count           = _enrichment_count,
      deleted_count              = _deleted_count,
      ajour_date                 = now();

  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_records_summary()
  RETURNS TRIGGER AS $$ -- V23
DECLARE
  _original_count   NUMERIC;
  _enrichment_count NUMERIC;
  _deleted_count    NUMERIC;
BEGIN
  SELECT
    rs.original_count,
    rs.enrichment_count,
    rs.deleted_count
  INTO _original_count, _enrichment_count, _deleted_count
  FROM records_summary rs
  WHERE rs.agencyid = NEW.agencyid
  FOR UPDATE;

  IF OLD.deleted <> NEW.deleted AND OLD.deleted -- Restore record
  THEN
    IF NEW.mimetype = 'text/enrichment+marcxchange'
    THEN
      _enrichment_count := _enrichment_count + 1;
    ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
      THEN
        _original_count := _original_count + 1;
    END IF;

    _deleted_count := _deleted_count - 1;
  ELSEIF OLD.deleted <> NEW.deleted AND NEW.deleted -- Delete record
    THEN
      IF OLD.mimetype = 'text/enrichment+marcxchange'
      THEN
        _enrichment_count := _enrichment_count - 1;
      ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
        THEN
          _original_count := _original_count - 1;
      END IF;

      _deleted_count := _deleted_count + 1;
  END IF;

  UPDATE records_summary
  SET original_count = _original_count,
    enrichment_count = _enrichment_count,
    deleted_count    = _deleted_count,
    ajour_date       = now()
  WHERE agencyid = NEW.agencyid;

  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_records_summary()
  RETURNS TRIGGER AS $$ -- V23
DECLARE
  _original_count   NUMERIC;
  _enrichment_count NUMERIC;
  _deleted_count    NUMERIC;
BEGIN
  SELECT
    rs.original_count,
    rs.enrichment_count,
    rs.deleted_count
  INTO _original_count, _enrichment_count, _deleted_count
  FROM records_summary rs
  WHERE rs.agencyid = OLD.agencyid
  FOR UPDATE;

  IF OLD.deleted
  THEN
    _deleted_count := _deleted_count - 1;
  ELSEIF OLD.mimetype = 'text/enrichment+marcxchange'
    THEN
      _enrichment_count := _enrichment_count - 1;
  ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
    THEN
      _original_count := _original_count - 1;
  END IF;

  IF _original_count = 0 AND _enrichment_count = 0 AND _deleted_count = 0
  THEN
    -- Clean up records_summary
    -- If this is the last row for the agency then remove that agency from the summary
    DELETE FROM records_summary
    WHERE agencyid = OLD.agencyid;
  ELSE
    UPDATE records_summary
    SET original_count = _original_count,
      enrichment_count = _enrichment_count,
      deleted_count    = _deleted_count,
      ajour_date       = now()
    WHERE agencyid = OLD.agencyid;
  END IF;

  RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER records_insert_trig_summary
  -- V23
  AFTER INSERT
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE insert_records_summary();

CREATE TRIGGER records_update_trig_summary
  -- V23
  AFTER UPDATE
  ON records
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE update_records_summary();

CREATE TRIGGER records_delete_trig_summary
  -- V23
  AFTER DELETE
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE delete_records_summary();


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
$$
LANGUAGE plpgsql;

CREATE TRIGGER relation_immutable_false_insert
  -- V10
  BEFORE INSERT
  ON relations
  FOR EACH ROW
EXECUTE PROCEDURE relation_immutable_false();

CREATE TRIGGER relation_immutable_false_update
  -- V10
  BEFORE UPDATE
  ON relations
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
  priority              NUMERIC(4)               NOT NULL DEFAULT 1000,
  CONSTRAINT queue_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker)
  -- NO primary key
  -- if it's claimed by worker
  -- a new job should be reinserted
);


CREATE TABLE jobdiag (-- V17
  bibliographicrecordid VARCHAR(64)              NOT NULL,
  agencyid              NUMERIC(6)               NOT NULL,
  worker                VARCHAR(32)              NOT NULL, -- name of designated worker
  error                 TEXT                     NOT NULL, -- errormessage
  queued                TIMESTAMP WITH TIME ZONE NOT NULL, -- timestamp for when it has been put into the queue
  priority              NUMERIC(4)               NOT NULL DEFAULT 1000
  -- NO primary key
  -- if it's claimed by worker
  -- a new job should be reinserted
);

--
-- Rules to tell which workses should get which types of events
-- When a given provider queues a job
--
CREATE TABLE queuerules (-- V18
  provider    VARCHAR(32) NOT NULL, -- name of worker adding data
  worker      VARCHAR(32) NOT NULL, -- name of designated worker
  changed     CHAR(1)     NOT NULL, -- queue jobs if changes Y(es), N(no), A(ll)
  leaf        CHAR(1)     NOT NULL, -- queue jobs if leaf    Y(es), N(no), A(ll),
  description VARCHAR(2000), -- human readable description of the provider and what it is used for
  -- changed AND leaf should be true to queue
  CONSTRAINT queuerules_pk PRIMARY KEY (provider, worker, changed, leaf),
  CONSTRAINT queuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker)
);


CREATE INDEX queue_idx_job
  ON queue (bibliographicrecordid, agencyid, worker);
CREATE INDEX queue_idx_worker
  ON queue (worker, priority, queued); --V4, V22
CREATE INDEX jobdiag_idx
  ON jobdiag (worker, error, queued, priority); --V7, V22
-- DROP TYPE enqueueResult;
CREATE TYPE ENQUEUERESULT AS (worker VARCHAR(32), queued BOOLEAN);

CREATE TABLE provider_log (-- V22
  provider  VARCHAR(32),
  hit_count NUMERIC   NOT NULL DEFAULT 1,
  modified  TIMESTAMP NOT NULL DEFAULT now()
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
$$
LANGUAGE plpgsql;


--- DEPRECATED as of December 2017
CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128),
                                   provider_              VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1))
  RETURNS SETOF VARCHAR(32) AS $$ -- V3, V8, V22
BEGIN
  SELECT *
  FROM enqueue(bibliographicrecordid_, agencyid_, provider_, changed_, leaf_, 1000);
END
$$
LANGUAGE plpgsql;


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
$$
LANGUAGE plpgsql;


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
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128))
  RETURNS SETOF QUEUE AS $$ -- V8
BEGIN
  RETURN QUERY
  SELECT *
  FROM dequeue(worker_, 1);
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128), no_ INT)
  RETURNS SETOF QUEUE AS $$ -- V8
DECLARE
  queue_row QUEUE;
BEGIN
  FOR queue_row IN SELECT *
                   FROM queue
                   WHERE worker = worker_
                   ORDER BY priority, queued
                   FOR UPDATE SKIP LOCKED
  LIMIT no_ LOOP
  BEGIN
    DELETE FROM queue
    WHERE bibliographicrecordid = queue_row.bibliographicrecordid
          AND agencyid = queue_row.agencyid
          AND worker = worker_;
    RETURN NEXT queue_row;
  END;
END LOOP;
END
$$
LANGUAGE plpgsql;