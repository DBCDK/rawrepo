--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 20;
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


CREATE TABLE messagequeuerules (
       worker VARCHAR(32) NOT NULL,            -- name of designated worker
       queuename VARCHAR(32) NOT NULL,         -- name of queue
       CONSTRAINT messagequeuerules_pk PRIMARY KEY (worker),
       CONSTRAINT messagequeuerules_fk_worker FOREIGN KEY (worker) REFERENCES queueworkers(worker),
       CONSTRAINT messagequeuerules_uniq_queuename UNIQUE (queuename)
);


CREATE OR REPLACE FUNCTION queues(provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$
BEGIN
    RETURN QUERY SELECT queuename FROM queuerules JOIN messagequeuerules USING(worker) WHERE provider=provider_ AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION messagequeuenames() RETURNS void AS $$
BEGIN
    DELETE FROM messagequeuerules;
    INSERT INTO messagequeuerules (worker, queuename) SELECT worker, SUBSTR(LOWER(worker),1,1) || SUBSTR(REPLACE(INITCAP(worker),'-',''),2) FROM queueworkers;
END
$$ LANGUAGE plpgsql;



--
--
--
COMMIT;
