
ALTER TABLE records ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

ALTER TABLE records_archive ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records_archive ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V2
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid ORDER BY modified DESC OFFSET 10 LIMIT 1
    LOOP
	DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

