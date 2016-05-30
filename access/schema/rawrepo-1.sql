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
DROP INDEX IF EXISTS relations_reverse;
DROP INDEX IF EXISTS records_modified;
DROP TABLE IF EXISTS relations;
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS records_archive;

-- records:
-- id, library => content(blob), created
CREATE TABLE records (
       id VARCHAR(64) NOT NULL,
       library NUMERIC(6) NOT NULL,
       content TEXT, -- base64 encoded - NULL is deleted
       created TIMESTAMP NOT NULL,
       modified TIMESTAMP NOT NULL,
       CONSTRAINT records_pk PRIMARY KEY (id, library)
);

CREATE TABLE records_archive (
       id VARCHAR(64) NOT NULL,
       library NUMERIC(6) NOT NULL,
       content TEXT, -- base64 encoded - NULL is deleted
       created TIMESTAMP NOT NULL,
       modified TIMESTAMP NOT NULL,
       CONSTRAINT records_archive_pk PRIMARY KEY (id, library, modified)
);
--
-- reverse index for getRelationsChildren()
CREATE INDEX records_modified ON records (modified);

-- relations:
-- id, library => refer(id, library)
CREATE TABLE relations (
       id VARCHAR(64) NOT NULL,
       library NUMERIC(6) NOT NULL,
       refer_id VARCHAR(64) NOT NULL,
       refer_library NUMERIC(6) NOT NULL,
       CONSTRAINT relations_pk PRIMARY KEY (id, library, refer_id, refer_library),
       CONSTRAINT relations_fk_owner FOREIGN KEY (id, library) REFERENCES records(id, library),
       CONSTRAINT relations_fk_refer FOREIGN KEY (refer_id, refer_library) REFERENCES records(id, library)
);

--
-- reverse index for getRelationsChildren()
CREATE INDEX relations_reverse ON relations (refer_id, refer_library);

--
-- QUEUE complex
--
DROP FUNCTION IF EXISTS dequeue(worker VARCHAR(128));
DROP FUNCTION IF EXISTS enqueue(id VARCHAR(64), library NUMERIC(6), worker VARCHAR(32), changed CHAR(1), leaf CHAR(1));

DROP INDEX IF EXISTS queue_idx_job;
DROP INDEX IF EXISTS queue_idx_worker;
DROP INDEX IF EXISTS queue_idx_queued;

DROP TABLE IF EXISTS queue;
DROP TABLE IF EXISTS queuerules;
DROP TABLE IF EXISTS queueworkers;

--
-- List of known workers and attributes to these
--
CREATE TABLE queueworkers (
       worker VARCHAR(32) NOT NULL,            -- name of designated worker
       CONSTRAINT queueworkers_pk PRIMARY KEY (worker)
);



CREATE TABLE queue (
       id VARCHAR(64) NOT NULL,
       library NUMERIC(6) NOT NULL,
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
CREATE TABLE queuerules (
       provider VARCHAR(32) NOT NULL, -- name of worker adding data
       worker VARCHAR(32) NOT NULL,   -- name of designated worker
       changed CHAR(1) NOT NULL,      -- queue jobs if changes Y(es), N(no), A(ll)
       leaf CHAR(1) NOT NULL,         -- queue jobs if leaf    Y(es), N(no), A(ll)
       -- changed AND leaf should be true to queue
       CONSTRAINT queuerules_pk PRIMARY KEY (provider, worker, changed, leaf),
       CONSTRAINT queuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers(worker)
);


CREATE INDEX queue_idx_job ON queue(id, library, worker, blocked);
CREATE INDEX queue_idx_worker ON queue(worker, blocked);
CREATE INDEX queue_idx_queued ON queue(queued);




CREATE OR REPLACE FUNCTION enqueue(id_ VARCHAR(64), library_ NUMERIC(6), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$
DECLARE
    row queuerules;
    exists queue;
    rows int;
BEGIN
    FOR row IN SELECT * FROM queuerules WHERE provider=provider_ AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_) LOOP
    	-- RAISE NOTICE 'worker=%', row.worker;
	SELECT COUNT(*) INTO rows FROM queue WHERE id=id_ AND library=library_ AND worker=row.worker AND blocked='';
	-- RAISE NOTICE 'rows=%', rows;
	CASE
	    WHEN rows = 0 THEN -- none is queued
	        INSERT INTO queue(id, library, worker) VALUES(id_, library_, row.worker);
	    WHEN rows = 1 THEN -- one is queued - but may be locked by a worker
	    	BEGIN
		    SELECT * INTO exists FROM queue WHERE id=id_ AND library=library_ AND worker=row.worker AND blocked='' FOR UPDATE NOWAIT;
                    -- By locking the row, we ensure that no worker can take this row until we commit / rollback
                    -- Ensuring that even if this job is next, it will not be processed until we're sure our data is used.
		EXCEPTION
	    	    WHEN lock_not_available THEN
                        INSERT INTO queue(id, library, worker) VALUES(id_, library_, row.worker);
		END;
	    ELSE
	        -- nothing
	END CASE;
    END LOOP;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dequeue(worker_ VARCHAR(128)) RETURNS SETOF queue AS $$
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
            FOR upd IN SELECT * FROM queue WHERE id=row.id AND library=row.library AND worker=worker_ AND blocked='' FOR UPDATE NOWAIT LOOP
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

