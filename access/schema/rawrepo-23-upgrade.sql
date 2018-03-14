--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 23;
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

ALTER TABLE queuerules
  ADD COLUMN description VARCHAR(2000);

CREATE TABLE records_summary (-- V23
  agencyid         NUMERIC(6) PRIMARY KEY   NOT NULL,
  original_count   NUMERIC                  NOT NULL DEFAULT 0,
  enrichment_count NUMERIC                  NOT NULL DEFAULT 0,
  deleted_count    NUMERIC                  NOT NULL DEFAULT 0,
  ajour_date       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Remember GRANT when patching boblebad and cisterne
-- GRANT select ON records_summary TO xxx_ro;
-- GRANT select, insert, update ON records_summary TO xxx_upd;

DROP TRIGGER archive_record_update
ON records;
DROP TRIGGER archive_record_delete
ON records;

DROP FUNCTION archive_record();
DROP FUNCTION archive_record_cleanup();

CREATE OR REPLACE FUNCTION update_records_archive()
  RETURNS TRIGGER AS $$ -- V23
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_records_archive()
  RETURNS TRIGGER AS $$ -- V23
BEGIN
  INSERT INTO records_archive (bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
  VALUES (OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified,
          OLD.trackingId);
  RETURN OLD;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_records_summary()
  RETURNS TRIGGER AS $$ -- V23
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
  ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
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
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_records_summary()
  RETURNS TRIGGER AS $$ -- V23
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
    ELSEIF NEW.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
      THEN
        _original_count := _original_count + 1;
    END IF;

    _deleted_count := _deleted_count - 1;
  ELSEIF OLD.deleted <> NEW.deleted AND NEW.deleted -- Delete record
    THEN
      IF OLD.mimetype = 'text/enrichment+marcxchange'
      THEN
        _enrichment_count := _enrichment_count - 1;
      ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
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
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_records_summary()
  RETURNS TRIGGER AS $$ -- V23
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
  ELSEIF OLD.mimetype IN ('text/marcxchange', 'text/article+marcxchange', 'text/authority+marcxchange')
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
$$
LANGUAGE plpgsql;

-- Since we are both adding triggers and selecting initial data from records we need to lock that table first to ensure
-- nothing happens until we are completely done
BEGIN TRANSACTION;
LOCK TABLE records IN ACCESS EXCLUSIVE MODE;

-- Insert triggers
CREATE TRIGGER records_insert_trig_summary
  -- V23
  AFTER INSERT
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE insert_records_summary();

-- Update triggers
CREATE TRIGGER records_update_trig_archive
  -- V23
  AFTER UPDATE
  ON records
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE update_records_archive();

CREATE TRIGGER records_update_trig_summary
  -- V23
  AFTER UPDATE
  ON records
  FOR EACH ROW
  WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE update_records_summary();

-- Delete triggers
-- Under normal production condition we will never delete rows in the record table
-- However, if it does happens it's nice to keep the archive table and summary table up to date
CREATE TRIGGER records_delete_trig_archive
  -- V23
  AFTER DELETE
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE delete_records_archive();

CREATE TRIGGER records_delete_trig_summary
  -- V23
  AFTER DELETE
  ON records
  FOR EACH ROW
EXECUTE PROCEDURE delete_records_summary();

-- Populate the records_summary table
INSERT INTO records_summary
  SELECT
    agencyid,
    count(*)
      FILTER (WHERE deleted = 'F' AND mimetype IN
                                      ('text/marcxchange',
                                       'text/article+marcxchange',
                                       'text/authority+marcxchange')),
    count(*)
      FILTER (WHERE deleted = 'F' AND mimetype = 'text/enrichment+marcxchange'),
    count(*)
      FILTER (WHERE deleted = 'T'),
    max(modified)
  FROM records
  GROUP BY agencyid
  ORDER BY agencyid;

COMMIT TRANSACTION;