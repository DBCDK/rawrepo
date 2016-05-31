/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.marcxmerge;

/**
 *
 * @author DBC <dbc.dk>
 */
public class MarcXChangeMimeType {

    public static final String MARCXCHANGE = "text/marcxchange";
    public static final String ENRICHMENT = "text/enrichment+marcxchange";
    public static final String AUTHORITY = "text/authority+marcxchange";

    public static boolean isMarcXChange(String mimetype) {
        switch (mimetype) {
            case MARCXCHANGE:
            case AUTHORITY:
                return true;
            default:
                return false;
        }
    }

    public static boolean isEnrichment(String mimetype) {
        switch (mimetype) {
            case ENRICHMENT:
                return true;
            default:
                return false;
        }
    }
}
