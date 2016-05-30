-- 
-- dbc-rawrepo-access
-- Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
-- Denmark. CVR: 15149043
--
-- This file is part of dbc-rawrepo-access.
--
-- dbc-rawrepo-access is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- dbc-rawrepo-access is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
-- 

ALTER TABLE records ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

ALTER TABLE records_archive ADD COLUMN deleted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE records_archive ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT 'text/marcxchange';

UPDATE records SET content='', deleted=TRUE WHERE content IS NULL;
-- ALTER TABLE records ALTER COLUMN content SET NOT NULL;

UPDATE records_archive SET content='', deleted=TRUE WHERE content IS NULL;
-- ALTER TABLE records_archive ALTER COLUMN content SET NOT NULL;



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



CREATE OR REPLACE FUNCTION enqueue(bibliographicrecordid_ VARCHAR(64), agencyid_ NUMERIC(6), mimetype_ VARCHAR(128), provider_ VARCHAR(32), changed_ CHAR(1), leaf_ CHAR(1)) RETURNS SETOF VARCHAR(32) AS $$ -- V3
DECLARE
    row queuerules;
    exists queue;
    rows int;
BEGIN
    FOR row IN SELECT * FROM queuerules WHERE provider=provider_ AND (mimetype='' OR mimetype=mimetype_) AND (changed='A' OR changed=changed_) AND (leaf='A' OR leaf=leaf_) LOOP
    	-- RAISE NOTICE 'worker=%', row.worker;
	SELECT COUNT(*) INTO rows FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='';
	-- RAISE NOTICE 'rows=%', rows;
	CASE
	    WHEN rows = 0 THEN -- none is queued
	        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
	    WHEN rows = 1 THEN -- one is queued - but may be locked by a worker
	    	BEGIN
		    SELECT * INTO exists FROM queue WHERE bibliographicrecordid=bibliographicrecordid_ AND agencyid=agencyid_ AND worker=row.worker AND blocked='' FOR UPDATE NOWAIT;
                    -- By locking the row, we ensure that no worker can take this row until we commit / rollback
                    -- Ensuring that even if this job is next, it will not be processed until we're sure our data is used.
		EXCEPTION
	    	    WHEN lock_not_available THEN
                        INSERT INTO queue(bibliographicrecordid, agencyid, worker) VALUES(bibliographicrecordid_, agencyid_, row.worker);
		END;
	    ELSE
	        -- nothing
	END CASE;
    END LOOP;
END
$$ LANGUAGE plpgsql;


ALTER TABLE queuerules ADD COLUMN mimetype VARCHAR(128) NOT NULL DEFAULT '';




INSERT INTO version VALUES(3);
