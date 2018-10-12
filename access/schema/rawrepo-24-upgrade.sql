--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 24;
  brokenversion  INTEGER = 23;
  OLDversion     INTEGER;
BEGIN
  SELECT MAX(version)
  INTO OLDversion
  FROM version;
  IF (OLDversion <> (currentversion - 1))
  THEN
    RAISE EXCEPTION 'Expected schema version % found %', (currentversion - 1), OLDversion;
  END IF;
  INSERT INTO version VALUES (currentversion);
  DELETE FROM version
  WHERE version = brokenversion;
END
$$;

--
--
--

CREATE TABLE configurations (-- V23
  key VARCHAR PRIMARY KEY NOT NULL,
  value VARCHAR NOT NULL DEFAULT ''
);

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
  CONSTRAINT records_cache_pk PRIMARY KEY (bibliographicrecordid, agencyid, cachekey)
);

ALTER TABLE ONLY queue ALTER COLUMN priority SET DEFAULT 500;
ALTER TABLE ONLY jobdiag ALTER COLUMN priority SET DEFAULT 500;


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
                 500) AS e -- When bulk enqueuing we always want to use default priority
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



COMMIT TRANSACTION;