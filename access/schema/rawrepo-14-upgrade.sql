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

--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
DECLARE
  currentversion INTEGER = 14;
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

CREATE OR REPLACE FUNCTION archive_record() RETURNS TRIGGER AS $$ -- V12
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified, OLD.trackingId);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION archive_record_cleanup() RETURNS TRIGGER AS $$ -- V12
DECLARE
    ts TIMESTAMP;
BEGIN
    INSERT INTO records_archive(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingId)
        VALUES(OLD.bibliographicrecordid, OLD.agencyid, OLD.deleted, OLD.mimetype, OLD.content, OLD.created, OLD.modified, OLD.trackingId);
    FOR ts IN
        SELECT modified FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND
                                                   modified <=  NEW.modified - INTERVAL '42 DAYS' ORDER BY modified DESC OFFSET 1 LIMIT 1
    LOOP
        DELETE FROM records_archive WHERE bibliographicrecordid=OLD.bibliographicrecordid AND agencyid=OLD.agencyid AND modified<=ts;
    END LOOP;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER archive_record_update ON records;

CREATE TRIGGER archive_record_update
	AFTER UPDATE ON records
	FOR EACH ROW
	WHEN ((old.* IS DISTINCT FROM new.*))
	EXECUTE PROCEDURE archive_record_cleanup();

--
--
--
COMMIT;
