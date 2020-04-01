--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 28;
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

DROP TRIGGER records_insert_trig_summary ON records;
DROP TRIGGER records_update_trig_summary ON records;
DROP TRIGGER records_delete_trig_summary ON records;

DROP FUNCTION insert_records_summary();
DROP FUNCTION update_records_summary();
DROP FUNCTION delete_records_summary();

CREATE OR REPLACE FUNCTION refresh_records_summary()
    RETURNS SETOF records_summary AS $$
DECLARE
    agencyid_ NUMERIC;
    row records_summary;
BEGIN
    FOR agencyid_ IN SELECT DISTINCT(agencyid) FROM records LOOP
            SELECT agencyid,
                   count(*) FILTER (WHERE deleted = 'F' AND mimetype != 'text/enrichment+marcxchange') AS original_count,
                   count(*) FILTER (WHERE deleted = 'F' AND mimetype = 'text/enrichment+marcxchange') AS enrichment_count,
                   count(*) FILTER (WHERE deleted = 'T') AS deleted_count,
                   max(modified) AS ajour_date
            INTO row
            FROM records
            WHERE agencyid = agencyid_
            GROUP BY agencyid
            ORDER BY agencyid;

            INSERT INTO records_summary (agencyId,
                                         original_count,
                                         enrichment_count,
                                         deleted_count,
                                         ajour_date)
            VALUES (row.agencyid,
                    row.original_count,
                    row.enrichment_count,
                    row.deleted_count,
                    row.ajour_date)
            ON CONFLICT (agencyid)
                DO UPDATE SET original_count = row.original_count,
                              enrichment_count = row.enrichment_count,
                              deleted_count = row.deleted_count,
                              ajour_date = row.ajour_date;
        END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR replace FUNCTION refresh_records_summary_by_agencyId(agencyid_ NUMERIC(6))
    RETURNS SETOF records_summary AS $$ -- V28
DECLARE
    row    records_summary;
BEGIN
    SELECT agencyId,
           count(*) FILTER (WHERE deleted = 'F' AND mimetype != 'text/enrichment+marcxchange') AS original_count,
           count(*) FILTER (WHERE deleted = 'F' AND mimetype = 'text/enrichment+marcxchange') AS enrichment_count,
           count(*) FILTER (WHERE deleted = 'T') AS deleted_count,
           max(modified) AS ajour_date
    INTO row
    FROM records
    WHERE agencyId = agencyid_
    GROUP BY agencyid
    ORDER BY agencyid;

    INSERT INTO records_summary (agencyId, original_count, enrichment_count, deleted_count, ajour_date) VALUES (agencyid_, row.original_count , row.enrichment_count, row.deleted_count, row.ajour_date)
    ON CONFLICT (agencyid)
        DO UPDATE SET original_count = row.original_count,
                      enrichment_count = row.enrichment_count,
                      deleted_count = row.deleted_count,
                      ajour_date = row.ajour_date;

    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMIT TRANSACTION;