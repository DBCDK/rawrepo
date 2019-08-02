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

CREATE OR REPLACE FUNCTION delete_records_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ -- V23
DECLARE
  _original_count   NUMERIC;
  _enrichment_count NUMERIC;
  _deleted_count    NUMERIC;
BEGIN
  SELECT
    rs.original_count,
    rs.enrichment_count,
    rs.deleted_count
  INTO _original_count, _enrichment_count, _deleted_count
  FROM records_summary rs
  WHERE rs.agencyid = OLD.agencyid
  FOR UPDATE;

  IF OLD.deleted
  THEN
    _deleted_count := _deleted_count - 1;
  ELSEIF OLD.mimetype = 'text/enrichment+marcxchange'
    THEN
      _enrichment_count := _enrichment_count - 1;
  ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange', 'text/litanalysis+marcxchange')
    THEN
      _original_count := _original_count - 1;
  END IF;

  IF _original_count = 0 AND _enrichment_count = 0 AND _deleted_count = 0
  THEN
    -- Clean up records_summary
    -- If this is the last row for the agency then remove that agency from the summary
    DELETE FROM records_summary
    WHERE agencyid = OLD.agencyid;
  ELSE
    UPDATE records_summary
    SET original_count = _original_count,
      enrichment_count = _enrichment_count,
      deleted_count    = _deleted_count,
      ajour_date       = now()
    WHERE agencyid = OLD.agencyid;
  END IF;

  RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION insert_records_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ -- V23
DECLARE
  _original_count   NUMERIC := 0;
  _enrichment_count NUMERIC := 0;
  _deleted_count    NUMERIC := 0;
  _summary_count    NUMERIC;
BEGIN
  -- Since this is an insert it might be the first record for that agency, so we can't be sure there is a row for that
  -- agency in records_summary
  SELECT count(*)
  INTO _summary_count
  FROM records_summary
  WHERE records_summary.agencyid = NEW.agencyid;

  IF _summary_count > 0
  THEN
    SELECT
      rs.original_count,
      rs.enrichment_count,
      rs.deleted_count
    INTO _original_count, _enrichment_count, _deleted_count
    FROM records_summary rs
    WHERE rs.agencyid = NEW.agencyid
    FOR UPDATE;
  END IF;

  IF NEW.deleted
  THEN -- Handled deleted record (this probably won't happen, but just to be sure...)
    _deleted_count := _deleted_count + 1;
  ELSEIF NEW.mimetype = 'text/enrichment+marcxchange'
    THEN -- Handle enrichment
      _enrichment_count := _enrichment_count + 1;
  ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange', 'text/litanalysis+marcxchange')
    THEN -- Handle original record
      _original_count := _original_count + 1;
  END IF;

  INSERT INTO records_summary (agencyid, original_count, enrichment_count, deleted_count, ajour_date)
  VALUES (NEW.agencyid, _original_count, _enrichment_count, _deleted_count, now())
  ON CONFLICT (agencyid)
    DO UPDATE SET original_count = _original_count,
      enrichment_count           = _enrichment_count,
      deleted_count              = _deleted_count,
      ajour_date                 = now();

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION update_records_summary() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ -- V23
DECLARE
  _original_count   NUMERIC;
  _enrichment_count NUMERIC;
  _deleted_count    NUMERIC;
BEGIN
  SELECT
    rs.original_count,
    rs.enrichment_count,
    rs.deleted_count
  INTO _original_count, _enrichment_count, _deleted_count
  FROM records_summary rs
  WHERE rs.agencyid = NEW.agencyid
  FOR UPDATE;

  IF OLD.deleted <> NEW.deleted AND OLD.deleted -- Restore record
  THEN
    IF NEW.mimetype = 'text/enrichment+marcxchange'
    THEN
      _enrichment_count := _enrichment_count + 1;
    ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange', 'text/litanalysis+marcxchange')
      THEN
        _original_count := _original_count + 1;
    END IF;

    _deleted_count := _deleted_count - 1;
  ELSEIF OLD.deleted <> NEW.deleted AND NEW.deleted -- Delete record
    THEN
      IF OLD.mimetype = 'text/enrichment+marcxchange'
      THEN
        _enrichment_count := _enrichment_count - 1;
      ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange', 'text/litanalysis+marcxchange')
        THEN
          _original_count := _original_count - 1;
      END IF;

      _deleted_count := _deleted_count + 1;
  END IF;

  UPDATE records_summary
  SET original_count = _original_count,
    enrichment_count = _enrichment_count,
    deleted_count    = _deleted_count,
    ajour_date       = now()
  WHERE agencyid = NEW.agencyid;

  RETURN NEW;
END;
$$;

COMMIT TRANSACTION;