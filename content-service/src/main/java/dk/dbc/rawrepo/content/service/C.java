/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class C {

    public static final String NS = "http://oss.dbc.dk/ns/rawreposervice";
    public static final String SERVICE = "RawRepoContentService";
    public static final String VERSION = "1.0";

    public static final String PORT = SERVICE + "/" + VERSION;

    public static final String OPERATION_FETCH = "fetch";

    public static final String X_FORWARDED_FOR = "x-forwarded-for";

    public static class FORS {
        public static final String URL = "FORSRIGHTS_URL";

        public static final String CONNECT_TIMEOUT = "FORSRIGHTS_CONNECT_TIMEOUT";
        public static final String CONNECT_TIMEOUT_DEFAULT = "2500";
        public static final String REQUEST_TIMEOUT = "FORSRIGHTS_REQUEST_TIMEOUT";
        public static final String REQUEST_TIMEOUT_DEFAULT = "10000";
        public static final String CACHE = "FORSRIGHTS_CACHE";
        public static final String CACHE_DEFAULT = "7200";
        public static final String DISABLED = "FORSRIGHTS_DISABLED";
        public static final String DISABLED_DEFAULT = "false";
        public static final String RIGHTS_NAME = "FORSRIGHTS_NAME";
        public static final String RIGHTS_NAME_DEFAULT = "*";
        public static final String RIGHTS_RIGHT = "FORSRIGHTS_RIGHT";
        public static final String RIGHTS_RIGHT_DEFAULT = "*";
    }

    public static class OPENAGENCY {
        public static final String URL = "OPENAGENCY_URL";

        public static final String CONNECT_TIMEOUT = "OPENAGENCY_CONNECT_TIMEOUT";
        public static final String CONNECT_TIMEOUT_DEFAULT = "2500";
        public static final String REQUEST_TIMEOUT = "OPENAGENCY_REQUEST_TIMEOUT";
        public static final String REQUEST_TIMEOUT_DEFAULT = "10000";
        public static final String CACHE_AGE = "OPENAGENCY_CACHE_AGE";
        public static final String CACHE_AGE_DEFAULT = String.valueOf(8);
    }

}
