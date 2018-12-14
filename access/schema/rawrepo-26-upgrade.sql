--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 26;
  brokenversion  INTEGER = 20;
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

CREATE OR REPLACE FUNCTION upsert_records_cache(
  _bibliographicrecordid VARCHAR(64),
  _agencyid              NUMERIC(6),
  _cachekey              TEXT,
  _deleted               BOOLEAN, -- V3
  _mimetype              VARCHAR(128), -- V3
  _content               TEXT, -- base64 encoded
  _created               TIMESTAMP WITH TIME ZONE,
  _modified              TIMESTAMP WITH TIME ZONE,
  _trackingId            VARCHAR(256),
  _enrichmenttrail       TEXT
)
  RETURNS VOID AS $$ -- V18
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
$$
LANGUAGE plpgsql;

COMMIT TRANSACTION;