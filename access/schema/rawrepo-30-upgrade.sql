--
-- ENSURE ONLY UPGRADING PREVIOUS VERSION
--
\set ON_ERROR_STOP

BEGIN TRANSACTION;

DO
$$
    DECLARE
        currentversion INTEGER = 30;
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

DROP INDEX jobdiag_idx;

CREATE INDEX jobdiag_idx
    ON jobdiag (worker, queued, priority); --V7, V22, V30

COMMIT TRANSACTION;
