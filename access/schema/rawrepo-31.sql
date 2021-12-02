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
INSERT INTO version VALUES (24);
INSERT INTO version VALUES (25);
INSERT INTO version VALUES (26);
INSERT INTO version VALUES (27);
INSERT INTO version VALUES (28);
INSERT INTO version VALUES (29);
INSERT INTO version VALUES (30);
INSERT INTO version VALUES (31);

CREATE TABLE configurations (-- V23
                                key VARCHAR PRIMARY KEY NOT NULL,
                                value VARCHAR NOT NULL DEFAULT ''
);

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

CREATE TABLE records_cache (-- V2
                               bibliographicrecordid VARCHAR(64)              NOT NULL,
                               agencyid              NUMERIC(6)               NOT NULL,
                               cachekey              TEXT                     NOT NULL,
                               deleted               BOOLEAN                  NOT NULL DEFAULT FALSE, -- V3
                               mimetype              VARCHAR(128)             NOT NULL DEFAULT 'text/marcxchange', -- V3
                               content               TEXT, -- base64 encoded
                               created               TIMESTAMP WITH TIME ZONE NOT NULL,
                               modified              TIMESTAMP WITH TIME ZONE NOT NULL,
                               trackingId            VARCHAR(256)             NOT NULL DEFAULT '',
                               enrichmenttrail       TEXT,
                               CONSTRAINT records_cache_pk PRIMARY KEY (bibliographicrecordid, agencyid, cachekey)
);

CREATE TABLE records_archive (-- V2
                                 bibliographicrecordid VARCHAR(64)              NOT NULL,
                                 agencyid              NUMERIC(6)               NOT NULL,
                                 deleted               BOOLEAN                  NOT NULL DEFAULT FALSE, -- V3
                                 mimetype              VARCHAR(128)             NOT NULL DEFAULT 'text/marcxchange', -- V3
                                 content               TEXT, -- base64 encoded
                                 created               TIMESTAMP WITH TIME ZONE NOT NULL,
                                 modified              TIMESTAMP WITH TIME ZONE NOT NULL,
                                 trackingId            VARCHAR(256)             NOT NULL DEFAULT '',
                                 CONSTRAINT records_archive_pkey PRIMARY KEY (bibliographicrecordid, agencyid, modified)
);
-- Primary key is the same as the records table plus 'modified'.

--
-- index for looking up records in archive
CREATE INDEX records_archive_id
    ON records_archive (bibliographicrecordid, agencyid);
CREATE INDEX records_archive_modified
    ON records_archive (modified);

CREATE OR REPLACE FUNCTION update_records_archive() RETURNS trigger
    LANGUAGE plpgsql
AS $$ -- V23
BEGIN
    INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
    VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
            OLD.trackingId);
    RETURN NEW;
END;
$$;


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
-- records_summary
--
CREATE TABLE records_summary (-- V23
                                 agencyid         NUMERIC(6) PRIMARY KEY   NOT NULL,
                                 original_count   NUMERIC                  NOT NULL DEFAULT 0,
                                 enrichment_count NUMERIC                  NOT NULL DEFAULT 0,
                                 deleted_count    NUMERIC                  NOT NULL DEFAULT 0,
                                 ajour_date       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION refresh_records_summary() RETURNS SETOF public.records_summary
    LANGUAGE plpgsql
AS $$
DECLARE
    row records_summary;
BEGIN
    FOR row  IN
        SELECT agencyid,
               count(*) FILTER (WHERE deleted = 'F' AND mimetype != 'text/enrichment+marcxchange') AS original_count,
               count(*) FILTER (WHERE deleted = 'F' AND mimetype = 'text/enrichment+marcxchange') AS enrichment_count,
               count(*) FILTER (WHERE deleted = 'T') AS deleted_count,
               max(modified) AS ajour_date
        FROM records
        GROUP BY agencyid
        ORDER BY agencyid
        LOOP
            INSERT INTO records_summary (agencyId,
                                         original_count,
                                         enrichment_count,
                                         deleted_count,
                                         ajour_date)
            VALUES (row.agencyid,
                    row.original_count,
                    row.enrichment_count,
                    row.deleted_count,
                    row.ajour_date)
            ON CONFLICT (agencyid)
                DO UPDATE SET original_count = row.original_count,
                              enrichment_count = row.enrichment_count,
                              deleted_count = row.deleted_count,
                              ajour_date = row.ajour_date;
        END LOOP;
    RETURN;
END;
$$;


CREATE OR replace FUNCTION refresh_records_summary_by_agencyId(agencyid_ NUMERIC(6))
    RETURNS SETOF records_summary AS $$ -- V28
DECLARE
    row    records_summary;
BEGIN
    SELECT agencyId,
           count(*) FILTER (WHERE deleted = 'F' AND mimetype != 'text/enrichment+marcxchange') AS original_count,
           count(*) FILTER (WHERE deleted = 'F' AND mimetype = 'text/enrichment+marcxchange') AS enrichment_count,
           count(*) FILTER (WHERE deleted = 'T') AS deleted_count,
           max(modified) AS ajour_date
    INTO row
    FROM records
    WHERE agencyId = agencyid_
    GROUP BY agencyid
    ORDER BY agencyid;

    INSERT INTO records_summary (agencyId, original_count, enrichment_count, deleted_count, ajour_date) VALUES (agencyid_, row.original_count , row.enrichment_count, row.deleted_count, row.ajour_date)
    ON CONFLICT (agencyid)
        DO UPDATE SET original_count = row.original_count,
                      enrichment_count = row.enrichment_count,
                      deleted_count = row.deleted_count,
                      ajour_date = row.ajour_date;

    RETURN;
END;
$$ LANGUAGE plpgsql;

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
CREATE OR REPLACE FUNCTION relation_immutable_false() RETURNS trigger
    LANGUAGE plpgsql
AS $$ -- V10
BEGIN
    NEW.always_false = FALSE;
    RETURN NEW;
END;
$$;


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
                       priority              NUMERIC(4)               NOT NULL DEFAULT 500,
                       id                    BIGSERIAL                NOT NULL, -- auto-incremented BIGINT with DEFAULT nextval for a sequence
                       CONSTRAINT queue_pkey PRIMARY KEY (id), -- Primary key is added to support live migration of the database, as that operation needs a primary key on all tables, but otherwise the key is not used
                       CONSTRAINT queue_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers (worker)
    -- if it's claimed by worker
    -- a new job should be reinserted
);


CREATE TABLE jobdiag (-- V17
                         bibliographicrecordid VARCHAR(64)              NOT NULL,
                         agencyid              NUMERIC(6)               NOT NULL,
                         worker                VARCHAR(32)              NOT NULL, -- name of designated worker
                         error                 TEXT                     NOT NULL, -- errormessage
                         queued                TIMESTAMP WITH TIME ZONE NOT NULL, -- timestamp for when it has been put into the queue
                         priority              NUMERIC(4)               NOT NULL DEFAULT 500,
                         id                    BIGSERIAL                NOT NULL, -- auto-incremented BIGINT with DEFAULT nextval for a sequence
                         CONSTRAINT jobdiag_pkey PRIMARY KEY (id) --Primary key is added to support live migration of the database, as that operation needs a primary key on all tables, but otherwise the key is not used
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
    ON jobdiag (worker, queued, priority); --V7, V22, V30
-- DROP TYPE enqueueResult;
CREATE TYPE ENQUEUERESULT AS (worker VARCHAR(32), queued BOOLEAN);

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


--- DEPRECATED as of December 2017
CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128),
                                   provider_              VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1))
    RETURNS SETOF VARCHAR(32) AS $$ -- V3, V8, V22
BEGIN
    SELECT *
    FROM enqueue(bibliographicrecordid_, agencyid_, provider_, changed_, leaf_, 500);
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
        FROM enqueue(bibliographicrecordid_, agencyid_, provider_, changed_, leaf_, 500);
END
$$
    LANGUAGE plpgsql;


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
                             leaf_ [elements_current],
                             1000) AS e
                LOOP
                    bibliographicrecordid = bibliographicrecordid_ [elements_current];
                    agencyid = agencyid_ [elements_current];
                    RETURN NEXT;
                END LOOP;

            elements_current = elements_current + 1;
        END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION enqueue_bulk(bibliographicrecordid_ character varying[], agencyid_ numeric[], provider_ character varying[], changed_ character varying[], leaf_ character varying[], priority_ numeric[]) RETURNS TABLE(bibliographicrecordid character varying, agencyid numeric, worker character varying, queued boolean)
    LANGUAGE plpgsql
AS $$ -- V31
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
                             priority_ [elements_current]) AS e
                LOOP
                    bibliographicrecordid = bibliographicrecordid_ [elements_current];
                    agencyid = agencyid_ [elements_current];
                    RETURN NEXT;
                END LOOP;

            elements_current = elements_current + 1;
        END LOOP;
END
$$;

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

CREATE OR REPLACE FUNCTION upsert_records_cache(_bibliographicrecordid character varying, _agencyid numeric, _cachekey text, _deleted boolean, _mimetype character varying, _content text, _created timestamp with time zone, _modified timestamp with time zone, _trackingid character varying, _enrichmenttrail text) RETURNS void
    LANGUAGE plpgsql
AS $$ -- V18
BEGIN
    INSERT INTO records_cache (bibliographicrecordid,
                               agencyid,
                               cachekey,
                               deleted,
                               mimetype,
                               content,
                               created,
                               modified,
                               trackingId,
                               enrichmentTrail)
    VALUES (_bibliographicrecordid,
            _agencyid,
            _cachekey,
            _deleted,
            _mimetype,
            _content,
            _created,
            _modified,
            _trackingId,
            _enrichmentTrail)
    ON CONFLICT (bibliographicrecordid, agencyid, cachekey)
        DO UPDATE SET deleted = _deleted,
                      mimetype            = _mimetype,
                      content             = _content,
                      created             = _created,
                      modified            = _modified,
                      trackingId          = _trackingId,
                      enrichmenttrail     = _enrichmenttrail;
END;
$$;
