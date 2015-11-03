/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-content-service
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class C {

    public static final String NS = "http://oss.dbc.dk/ns/rawreposervice";
    public static final String SERVICE = "RawRepoContentService";
    public static final String VERSION = "1.0";

    public static final String PORT = SERVICE + "/" + VERSION;
    public static final String ACTION_PATH = NS + "/" + SERVICE + "/";

    public static final String OPERATION_FETCH = "fetch";

    public static final String PROPERTIES_LOOKUP = "rawrepo-content-service";

    public static final String X_FORWARDED_FOR = "x-forwarded-for";

    public static class FORS {

        public static final String URL = "forsRightsUrl";
        public static final String URL_DEFAULT = "http://forsrights.addi.dk/1.2/";

        public static final String CONNECT_TIMEOUT = "forsRightsConnectTimeout";
        public static final String CONNECT_TIMEOUT_DEFAULT = "2500";
        public static final String REQUEST_TIMEOUT = "forsRightsRequestTimeout";
        public static final String REQUEST_TIMEOUT_DEFAULT = "10000";

        public static final String CACHE = "forsRightsCache";
        public static final String CACHE_DEFAULT = "7200";

        public static final String DISABLED = "forsRightsDisabled";
        public static final String DISABLED_DEFAULT = "false";

        public static final String RIGHTS_NAME = "forsRightsName";
        public static final String RIGHTS_NAME_DEFAULT = "*";
        public static final String RIGHTS_RIGHT = "forsRightsRight";
        public static final String RIGHTS_RIGHT_DEFAULT = "*";

    }

    public static class RAWREPO {

        public static final String DATASOURCE = "jdbc/rawrepocontentservice/rawrepo";
    }

    public static class SEARCHORDER {

        public static final String URL = "searchOrderUrl";
        public static final String URL_DEFAULT = "http://openagency.addi.dk/2.20/";

        public static final String CONNECT_TIMEOUT = "searchOrderConnectTimeout";
        public static final String CONNECT_TIMEOUT_DEFAULT = "2500";
        public static final String REQUEST_TIMEOUT = "searchOrderRequestTimeout";
        public static final String REQUEST_TIMEOUT_DEFAULT = "10000";

    }

}
