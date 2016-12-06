--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 19;
  brokenversion INTEGER = 0;
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
