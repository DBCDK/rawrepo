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

CREATE TABLE version (
       version NUMERIC(6) NOT NULL PRIMARY KEY
);


ALTER TABLE records RENAME COLUMN id TO bibliographicrecordid;
ALTER TABLE records RENAME COLUMN library TO agencyid;

ALTER TABLE records_archive RENAME COLUMN id TO bibliographicrecordid;
ALTER TABLE records_archive RENAME COLUMN library TO agencyid;
ALTER TABLE records_archive DROP CONSTRAINT records_archive_pk;


ALTER TABLE relations RENAME COLUMN id TO bibliographicrecordid;
ALTER TABLE relations RENAME COLUMN library TO agencyid;

ALTER TABLE relations RENAME COLUMN refer_id TO refer_bibliographicrecordid;
ALTER TABLE relations RENAME COLUMN refer_library TO refer_agencyid;

ALTER TABLE queue RENAME COLUMN id TO bibliographicrecordid;
ALTER TABLE queue RENAME COLUMN library TO agencyid;

DROP INDEX records_modified;
CREATE INDEX records_archive_id ON records_archive USING btree (bibliographicrecordid, agencyid);
CREATE INDEX records_archive_modified ON records_archive USING btree (modified);
CREATE INDEX records_archive_pk ON records_archive USING btree (bibliographicrecordid, agencyid, modified);

CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive VALUES(OLD.*);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid ORDER BY modified DESC OFFSET 10 LIMIT 1
    LOOP
	DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER archive_record_update
    AFTER UPDATE ON records
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE archive_record();

CREATE TRIGGER archive_record_delete
    AFTER DELETE ON records
    FOR EACH ROW
    EXECUTE PROCEDURE archive_record();



DROP FUNCTION enqueue(character varying,numeric,character varying,character,character);
CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$
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


INSERT INTO version VALUES(2);
