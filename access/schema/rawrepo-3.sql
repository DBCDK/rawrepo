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

CREATE TABLE version ( -- V2
       version NUMERIC(6) NOT NULL PRIMARY KEY
);
-- Compatible versions
INSERT INTO version VALUES(2);
INSERT INTO version VALUES(3);


-- records:
-- Primary objective: bibliographicrecordid, agencyid => content(blob)
CREATE TABLE records ( -- V2
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       deleted BOOLEAN NOT NULL DEFAULT FALSE, -- V3
       mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange', -- V3
       content TEXT, -- base64 encoded
       created TIMESTAMP NOT NULL,
       modified TIMESTAMP NOT NULL,
       CONSTRAINT records_pk PRIMARY KEY (bibliographicrecordid, agencyid)
);

CREATE TABLE records_archive ( -- V2
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       deleted BOOLEAN NOT NULL DEFAULT FALSE, -- V3
       mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange', -- V3
       content TEXT, -- base64 encoded
       created TIMESTAMP NOT NULL,
       modified TIMESTAMP NOT NULL
);



--
-- index for looking up records in archive
CREATE INDEX records_archive_pk ON records_archive (bibliographicrecordid, agencyid, modified);
CREATE INDEX records_archive_id ON records_archive (bibliographicrecordid, agencyid);
CREATE INDEX records_archive_modified ON records_archive (modified);

CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V2
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND
                                                   modified <=  NOW() - INTERVAL '42 DAYS' ORDER BY modified DESC OFFSET 1 LIMIT 1
    LOOP
	DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER archive_record_update -- V2
    AFTER UPDATE ON records
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE archive_record();

CREATE TRIGGER archive_record_delete -- V2
    AFTER DELETE ON records
    FOR EACH ROW
    EXECUTE PROCEDURE archive_record();



-- relations:
-- bibliographicrecordid, agencyid => refer(bibliographicrecordid, agencyid)
CREATE TABLE relations ( -- V2
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       refer_bibliographicrecordid VARCHAR(64) NOT NULL,
       refer_agencyid NUMERIC(6) NOT NULL,
       CONSTRAINT relations_pk PRIMARY KEY (bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid)
--V3       CONSTRAINT relations_fk_owner FOREIGN KEY (bibliographicrecordid, agencyid) REFERENCES records(bibliographicrecordid, agencyid),
--V3       CONSTRAINT relations_fk_refer FOREIGN KEY (refer_bibliographicrecordid, refer_agencyid) REFERENCES records(bibliographicrecordid, agencyid)
);

--
-- Validate relation to not deleted record
--
CREATE OR REPLACE FUNCTION validate_relation_fk() RETURNS TRIGGER AS $$ -- V3
DECLARE
    deletedRecord BOOLEAN;
BEGIN
    SELECT COUNT(*) = CAST(1 AS BIGINT) INTO deletedRecord FROM records WHERE bibliographicrecordid=NEW.bibliographicrecordid AND agencyid=NEW.agencyid AND deleted=TRUE;
    IF deletedRecord THEN
        RAISE EXCEPTION 'RELATION FROM DELETED';
    END IF;
    SELECT COUNT(*) = CAST(1 AS BIGINT) INTO deletedRecord FROM records WHERE bibliographicrecordid=NEW.refer_bibliographicrecordid AND agencyid=NEW.refer_agencyid AND deleted=TRUE;
    IF deletedRecord THEN
        RAISE EXCEPTION 'RELATION TO DELETED';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_relation_fk_insert -- V3
    AFTER INSERT ON relations
    FOR EACH ROW
    EXECUTE PROCEDURE validate_relation_fk();

CREATE TRIGGER validate_relation_fk_update -- V3
    AFTER UPDATE ON relations
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE validate_relation_fk();


--
-- Validate relation to newly deleted record
--
CREATE OR REPLACE FUNCTION validate_relation_fk_rev() RETURNS TRIGGER AS $$ -- V3
DECLARE
    hasRelations BOOLEAN;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid OR
                                                                   refer_bibliographicrecordid=OLD.bibliographicrecordid AND refer_agencyid=OLD.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO DELETED (PURGED)';
        END IF;
    END IF;
    IF (TG_OP = 'UPDATE' AND (OLD.bibliographicrecordid <> NEW.bibliographicrecordid OR OLD.agencyid <> NEW.agencyid)) THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid OR
                                                                   refer_bibliographicrecordid=OLD.bibliographicrecordid AND refer_agencyid=OLD.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO OLD ID (ID CHANGED)';
        END IF;
    END IF;
    IF (TG_OP = 'UPDATE' AND NEW.deleted) THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=NEW.bibliographicrecordid AND agencyid=NEW.agencyid OR
                                                                   refer_bibliographicrecordid=NEW.bibliographicrecordid AND refer_agencyid=NEW.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO DELETED';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_relation_fk_rev_update -- V3
    AFTER UPDATE ON records
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE validate_relation_fk_rev();

CREATE TRIGGER validate_relation_fk_rev_delete -- V3
    AFTER DELETE ON records
    FOR EACH ROW
    EXECUTE PROCEDURE validate_relation_fk_rev();


--
-- reverse index for getRelationsChildren()
CREATE INDEX relations_reverse ON relations (refer_bibliographicrecordid, refer_agencyid);

--
-- QUEUE complex
--

--
-- List of known workers and attributes to these
--
CREATE TABLE queueworkers ( -- V1
       worker VARCHAR(32) NOT NULL,            -- name of designated worker
       CONSTRAINT queueworkers_pk PRIMARY KEY (worker)
);



CREATE TABLE queue ( -- V2
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       worker VARCHAR(32) NOT NULL,              -- name of designated worker
       blocked VARCHAR(128) NOT NULL DEFAULT '', -- blocked for some reason
       queued TIMESTAMP NOT NULL DEFAULT timeofday()::timestamp,  -- timestamp for when it has been put into the queue
       CONSTRAINT queue_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers(worker)
-- NO primary key
-- if it's claimed by worker
-- a new job should be reinserted
);


--
-- Rules to tell which workses should get which types of events
-- When a given provider queues a job
--
CREATE TABLE queuerules ( -- V1
       provider VARCHAR(32) NOT NULL, -- name of worker adding data
       worker VARCHAR(32) NOT NULL,   -- name of designated worker
       mimetype VARCHAR(128) NOT NULL DEFAULT '', -- V3 queue jobs is mimetype ('' = ignore)
       changed CHAR(1) NOT NULL,      -- queue jobs if changes Y(es), N(no), A(ll)
       leaf CHAR(1) NOT NULL,         -- queue jobs if leaf    Y(es), N(no), A(ll)
       -- changed AND leaf should be true to queue
       CONSTRAINT queuerules_pk PRIMARY KEY (provider, worker, changed, leaf),
       CONSTRAINT queuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers(worker)
);


CREATE INDEX queue_idx_job ON queue(bibliographicrecordid, agencyid, worker, blocked);
CREATE INDEX queue_idx_worker ON queue(worker, blocked);
CREATE INDEX queue_idx_queued ON queue(queued);




CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$ -- V3
DECLARE
    row queuerules;
    exists queue;
    rows int;
BEGIN
    FOR row IN SELECT * FROM queuerules WHERE provider=provider_ AND (mimetype='' OR mimetype=mimetype_) AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_) LOOP
    	-- RAISE NOTICE 'worker=%', row.worker;
	SELECT COUNT(*) INTO rows FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='';
	-- RAISE NOTICE 'rows=%', rows;
	CASE
	    WHEN rows = 0 THEN -- none is queued
	        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
	    WHEN rows = 1 THEN -- one is queued - but may be locked by a worker
	    	BEGIN
		    SELECT * INTO exists FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='' FOR UPDATE NOWAIT;
                    -- By locking the row, we ensure that no worker can take this row until we commit / rollback
                    -- Ensuring that even if this job is next, it will not be processed until we're sure our data is used.
		EXCEPTION
	    	    WHEN lock_not_available THEN
                        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
		END;
	    ELSE
	        -- nothing
	END CASE;
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$ -- V2
DECLARE
    row queuerules;
    exists queue;
    rows int;
BEGIN
    FOR row IN SELECT * FROM queuerules WHERE provider=provider_ AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_) LOOP
    	-- RAISE NOTICE 'worker=%', row.worker;
	SELECT COUNT(*) INTO rows FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='';
	-- RAISE NOTICE 'rows=%', rows;
	CASE
	    WHEN rows = 0 THEN -- none is queued
	        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
	    WHEN rows = 1 THEN -- one is queued - but may be locked by a worker
	    	BEGIN
		    SELECT * INTO exists FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='' FOR UPDATE NOWAIT;
                    -- By locking the row, we ensure that no worker can take this row until we commit / rollback
                    -- Ensuring that even if this job is next, it will not be processed until we're sure our data is used.
		EXCEPTION
	    	    WHEN lock_not_available THEN
                        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
		END;
	    ELSE
	        -- nothing
	END CASE;
    END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128)) RETURNS SETOF queue AS $$ -- V2
DECLARE
 row queue;
 upd queue;
BEGIN
    <<done>>
    FOR row IN SELECT * FROM queue WHERE worker=worker_ AND blocked='' ORDER BY queued LOOP
        -- RAISE NOTICE 'job=%', row.job;
        BEGIN
	    -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
	    -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
	    -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
            FOR upd IN SELECT * FROM queue WHERE bibliographicrecordid=row.bibliographicrecordid AND agencyid=row.agencyid AND worker=worker_ AND blocked='' FOR UPDATE NOWAIT LOOP
                RETURN NEXT row;
                -- RAISE NOTICE 'job=%', row.job;
                EXIT done; -- We got one - exit
            END LOOP;
        EXCEPTION
            WHEN lock_not_available THEN
                -- RAISE NOTICE '% was locked', row.job;
        END;
    END LOOP;
END
$$ LANGUAGE plpgsql;

