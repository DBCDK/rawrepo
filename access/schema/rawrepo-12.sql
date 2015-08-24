\set ON_ERROR_STOP

CREATE TABLE version ( -- V2
       version NUMERIC(6) NOT NULL PRIMARY KEY,
       warning TEXT DEFAULT NULL -- V7
);
-- Compatible versions
INSERT INTO version VALUES(7);
INSERT INTO version VALUES(8);
INSERT INTO version VALUES(9);
INSERT INTO version VALUES(10);
INSERT INTO version VALUES(11);
INSERT INTO version VALUES(12);


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
       trackingId VARCHAR(256) NOT NULL DEFAULT '',
       CONSTRAINT records_pk PRIMARY KEY (bibliographicrecordid, agencyid)
);

CREATE UNIQUE INDEX records_relation_id ON records (bibliographicrecordid, agencyid, deleted); -- V10

CREATE TABLE records_archive ( -- V2
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       deleted BOOLEAN NOT NULL DEFAULT FALSE, -- V3
       mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange', -- V3
       content TEXT, -- base64 encoded
       created TIMESTAMP NOT NULL,
       modified TIMESTAMP NOT NULL,
       trackingId VARCHAR(256) NOT NULL DEFAULT ''
);



--
-- index for looking up records in archive
CREATE INDEX records_archive_pk ON records_archive (bibliographicrecordid, agencyid, modified);
CREATE INDEX records_archive_id ON records_archive (bibliographicrecordid, agencyid);
CREATE INDEX records_archive_modified ON records_archive (modified);

CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V12
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified, OLD.trackingId);
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
       always_false BOOLEAN NOT NULL DEFAULT false, -- V10
       CONSTRAINT relations_pk PRIMARY KEY (bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid),
       CONSTRAINT relations_fk_owner FOREIGN KEY (bibliographicrecordid, agencyid, always_false) REFERENCES records(bibliographicrecordid, agencyid, deleted),
       CONSTRAINT relations_fk_refer FOREIGN KEY (refer_bibliographicrecordid, refer_agencyid, always_false) REFERENCES records(bibliographicrecordid, agencyid, deleted)
);

--
-- Validate relation to not deleted record
--
CREATE OR REPLACE FUNCTION relation_immutable_false() RETURNS TRIGGER AS $$ -- V10
BEGIN
    NEW.always_false = false;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER relation_immutable_false_insert -- V10
    BEFORE INSERT ON relations
    FOR EACH ROW
    EXECUTE PROCEDURE relation_immutable_false();

CREATE TRIGGER relation_immutable_false_update -- V10
    BEFORE UPDATE ON relations
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE relation_immutable_false();

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
       queued TIMESTAMP NOT NULL DEFAULT timeofday()::timestamp,  -- timestamp for when it has been put into the queue
       CONSTRAINT queue_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers(worker)
-- NO primary key
-- if it's claimed by worker
-- a new job should be reinserted
);

CREATE TABLE jobdiag ( -- V7
       bibliographicrecordid VARCHAR(64) NOT NULL,
       agencyid NUMERIC(6) NOT NULL,
       worker VARCHAR(32) NOT NULL,  -- name of designated worker
       error VARCHAR(128) NOT NULL,  -- errormessage
       queued TIMESTAMP NOT NULL     -- timestamp for when it has been put into the queue
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


CREATE INDEX queue_idx_job ON queue(bibliographicrecordid, agencyid, worker);
CREATE INDEX queue_idx_worker ON queue(worker, queued); --V4
CREATE INDEX jobdiag_idx ON jobdiag(worker, error, queued); --V7




CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$ -- V3,V8
DECLARE
    row queuerules;
    exists queue;
    rows int;
BEGIN
    FOR row IN SELECT * FROM queuerules WHERE provider=provider_ AND (mimetype='' OR mimetype=mimetype_) AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_) LOOP
    	-- RAISE NOTICE 'worker=%', row.worker;
	SELECT COUNT(*) INTO rows FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker;
	-- RAISE NOTICE 'rows=%', rows;
	CASE
	    WHEN rows = 0 THEN -- none is queued
	        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
	    WHEN rows = 1 THEN -- one is queued - but may be locked by a worker
	    	BEGIN
		    SELECT * INTO exists FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker FOR UPDATE NOWAIT;
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



CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128)) RETURNS SETOF queue AS $$ -- V8
DECLARE
 row queue;
 upd queue;
BEGIN
    <<done>>
    -- LIMIT xxx CUT OFF TO LARGE DATA SETS, SHOULD BE > MAX WORKERTHREADS
    FOR row IN SELECT * FROM queue WHERE worker=worker_ ORDER BY queued LIMIT 256 LOOP
        BEGIN
	    -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
	    -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
	    -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
            FOR upd in SELECT * FROM queue WHERE bibliographicrecordid=row.bibliographicrecordid AND agencyid=row.agencyid AND worker=worker_ FOR UPDATE NOWAIT LOOP
                DELETE FROM queue WHERE bibliographicrecordid=row.bibliographicrecordid AND agencyid=row.agencyid AND worker=worker_;
                RETURN NEXT row;
                EXIT done; -- We got one - exit
            END LOOP;
        EXCEPTION
            WHEN lock_not_available THEN
        END;
    END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128), no_ INT) RETURNS SETOF queue AS $$ -- V8
DECLARE
 row queue;
 upd queue;
 no int = 0;
BEGIN
    <<done>>
    -- LIMIT xxx CUT OFF TO LARGE DATA SETS, SHOULD BE > MAX WORKERTHREADS
    FOR row IN SELECT * FROM queue WHERE worker=worker_ ORDER BY queued LOOP
        BEGIN
	    -- IF FIRST WITH THIS row.job IS TAKEN NONE WILL BE SELECTED
	    -- EVEN IF AN IDENTICAL IS LATER IN THE QUEUE
	    -- NO 2 WORKERS CAN RUN THE SAME JOB AT THE SAME TIME
            FOR upd in SELECT * FROM queue WHERE bibliographicrecordid=row.bibliographicrecordid AND agencyid=row.agencyid AND worker=worker_ FOR UPDATE NOWAIT LOOP
                DELETE FROM queue WHERE bibliographicrecordid=row.bibliographicrecordid AND agencyid=row.agencyid AND worker=worker_;
                RETURN NEXT row;
                no = no + 1;
                if no >= no_ THEN
                    EXIT done; -- We got one - exit
                END IF;
            END LOOP;
        EXCEPTION
            WHEN lock_not_available THEN
        END;
    END LOOP;
END
$$ LANGUAGE plpgsql;

