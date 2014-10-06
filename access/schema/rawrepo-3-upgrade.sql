
ALTER TABLE records ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

ALTER TABLE records_archive ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records_archive ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

UPDATE records SET content='', deleted=TRUE WHERE content IS NULL;
ALTER TABLE records ALTER COLUMN content SET NOT NULL;

UPDATE records_archive SET content='', deleted=TRUE WHERE content IS NULL;
ALTER TABLE records_archive ALTER COLUMN content SET NOT NULL;



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


ALTER TABLE relations DROP CONSTRAINT relations_fk_owner;

ALTER TABLE relations DROP CONSTRAINT relations_fk_refer;



CREATE OR REPLACE FUNCTION validate_relation_fk() RETURNS TRIGGER AS $$ -- V3
DECLARE
    deletedRecord BOOLEAN;
BEGIN
    SELECT COUNT(*) = CAST(1 AS BIGINT) INTO deletedRecord FROM records WHERE bibliographicrecordid=NEW.bibliographicrecordid AND agencyid=NEW.agencyid AND deleted=TRUE;
    IF deletedRecord THEN
        RAISE EXCEPTION 'RELATION FROM DELETED';
    END IF;
    SELECT COUNT(*) = CAST(1 AS BIGINT) INTO deletedRecord FROM records WHERE bibliographicrecordid=NEW.refer_bibliographicrecordid AND agencyid=NEW.refer_agencyid AND deleted=TRUE;
    IF deletedRecord THEN
        RAISE EXCEPTION 'RELATION TO DELETED';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_relation_fk_insert -- V3
    AFTER INSERT ON relations
    FOR EACH ROW
    EXECUTE PROCEDURE validate_relation_fk();

CREATE TRIGGER validate_relation_fk_update -- V3
    AFTER UPDATE ON relations
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE validate_relation_fk();


CREATE OR REPLACE FUNCTION validate_relation_fk_rev() RETURNS TRIGGER AS $$ -- V3
DECLARE
    hasRelations BOOLEAN;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid OR
                                                                   refer_bibliographicrecordid=OLD.bibliographicrecordid AND refer_agencyid=OLD.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO DELETED (PURGED)';
        END IF;
    END IF;
    IF (TG_OP = 'UPDATE' AND (OLD.bibliographicrecordid <> NEW.bibliographicrecordid OR OLD.agencyid <> NEW.agencyid)) THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid OR
                                                                   refer_bibliographicrecordid=OLD.bibliographicrecordid AND refer_agencyid=OLD.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO OLD ID (ID CHANGED)';
        END IF;
    END IF;
    IF (TG_OP = 'UPDATE' AND NEW.deleted) THEN
        SELECT COUNT(*) > 0 INTO hasRelations FROM relations WHERE bibliographicrecordid=NEW.bibliographicrecordid AND agencyid=NEW.agencyid OR
                                                                   refer_bibliographicrecordid=NEW.bibliographicrecordid AND refer_agencyid=NEW.agencyid;
        IF hasRelations THEN
            RAISE EXCEPTION 'RELATION TO DELETED';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_relation_fk_rev_update -- V3
    AFTER UPDATE ON records
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE validate_relation_fk_rev();

CREATE TRIGGER validate_relation_fk_rev_delete -- V3
    AFTER DELETE ON records
    FOR EACH ROW
    EXECUTE PROCEDURE validate_relation_fk_rev();

