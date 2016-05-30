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
  currentversion INTEGER = 10;
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

CREATE UNIQUE INDEX records_relation_id ON records (bibliographicrecordid, agencyid, deleted); -- V10


DROP TRIGGER record_delete_cascade ON records;
DROP TRIGGER validate_relation_fk_rev_delete ON records;
DROP TRIGGER validate_relation_fk_rev_update ON records;
DROP TRIGGER validate_relation_fk_insert ON relations;
DROP TRIGGER validate_relation_fk_update ON relations;

DROP FUNCTION record_delete_cascade();
DROP FUNCTION validate_relation_fk();
DROP FUNCTION validate_relation_fk_rev();

ALTER TABLE relations ADD COLUMN always_false boolean DEFAULT false NOT NULL;
ALTER TABLE relations ADD CONSTRAINT relations_fk_owner FOREIGN KEY (bibliographicrecordid, agencyid, always_false) REFERENCES records(bibliographicrecordid, agencyid, deleted);
ALTER TABLE relations ADD CONSTRAINT relations_fk_refer FOREIGN KEY (refer_bibliographicrecordid, refer_agencyid, always_false) REFERENCES records(bibliographicrecordid, agencyid, deleted);

CREATE OR REPLACE FUNCTION relation_immutable_false() RETURNS TRIGGER AS $$ -- V10
BEGIN
    NEW.always_false = false;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER relation_immutable_false_insert -- V10
    BEFORE INSERT ON relations
    FOR EACH ROW
    EXECUTE PROCEDURE relation_immutable_false();

CREATE TRIGGER relation_immutable_false_update -- V10
    BEFORE UPDATE ON relations
    FOR EACH ROW
    WHEN (OLD.* IS DISTINCT FROM NEW.*)
    EXECUTE PROCEDURE relation_immutable_false();

--
--
--
COMMIT;
