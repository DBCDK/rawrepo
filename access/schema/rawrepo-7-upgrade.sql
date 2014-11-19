--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 7;
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

ALTER TABLE version ADD COLUMN warning text;

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

CREATE INDEX jobdiag_idx ON jobdiag USING btree (worker, error, queued);

--
--
--
COMMIT;
