--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 8;
  brokenversion INTEGER = 6;
  oldversion INTEGER;
BEGIN
  SELECT MAX(version) INTO oldversion FROM version;
  IF (oldversion <> (currentversion-1)) THEN
    RAISE EXCEPTION 'Expected schema version % found %', (currentversion-1), oldversion;
  END IF;
  INSERT INTO version VALUES(currentversion);
  DELETE FROM version WHERE version <= brokenversion;
END
$$;



--
--
--
DROP INDEX queue_idx_job;
DROP INDEX queue_idx_worker;
ALTER TABLE queue DROP COLUMN blocked;
CREATE INDEX queue_idx_job ON queue(bibliographicrecordid, agencyid, worker); --V8
CREATE INDEX queue_idx_worker ON queue(worker, queued); --V8

DROP FUNCTION enqueue(bibliographicrecordid_ character varying, agencyid_ numeric, provider_ character varying, changed_ character, leaf_ character);


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


--
--
--
COMMIT;