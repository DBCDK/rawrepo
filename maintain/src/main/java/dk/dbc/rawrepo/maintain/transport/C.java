/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain.transport;

/**
 *
 * @author bogeskov
 */
public class C {

    public static final String NS = "http://rawrepo.dbc.dk/maintain/";
    public static final String SERVICE = "Maintain";
    public static final String VERSION = "1.0";

    public static final String PORT = SERVICE + "/" + VERSION;
    public static final String ACTION_PATH = NS + SERVICE + "/";

    public static final String DATASOURCE = "jdbc/rawrepomaintain/rawrepo";
    public static final String PROPERTIES = "rawrepo-maintain";

    public static final String OPERATION_PAGE_CONTENT = "pageContent";
    public static final String OPERATION_QUEUE_RECORDS = "queueRecords";
    public static final String OPERATION_REMOVE_RECORDS = "removeRecords";
    public static final String OPERATION_REVERT_RECORDS = "revertRecords";
}
