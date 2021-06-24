--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
    DECLARE
        currentversion INTEGER = 29;
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
        DELETE
        FROM version
        WHERE version = brokenversion;
    END
$$;

--
--
--

alter table queue
    add column id BIGSERIAL not null;
alter table queue
    add primary key (id);

alter table jobdiag
    add column id BIGSERIAL not null;
alter table jobdiag
    add primary key (id);

alter table records_archive
    add primary key (bibliographicrecordid, agencyid, modified);
drop index records_archive_pk;

COMMIT TRANSACTION;
