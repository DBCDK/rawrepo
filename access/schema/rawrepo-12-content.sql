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

---
--- rules for agency-delete
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('agency-delete', 'solr-sync', '', 'Y', 'A');


---
--- rules for fbs-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'solr-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('fbs-update', 'basis-decentral', '', 'Y', 'A');

---
--- rules for dataio-update
---
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'fbs-sync', '', 'Y', 'A');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'broend-sync', '', 'A', 'Y');
INSERT INTO queuerules (provider, worker, mimetype, changed, leaf) VALUES('dataio-update', 'solr-sync', '', 'Y', 'A');
