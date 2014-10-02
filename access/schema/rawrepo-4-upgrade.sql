CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V2
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND
                                                   modified <=  NOW() - INTERVAL '42 DAYS' ORDER BY modified DESC OFFSET 1 LIMIT 1
    LOOP
	DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
