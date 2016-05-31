/*
 * dbc-rawrepo-agency-load
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-load.
 *
 * dbc-rawrepo-agency-load is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-load is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-load.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencyload;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public abstract class MarcXProcessor {

    public abstract void marcxContent(String pos, String data);

    public abstract void marcxXml(byte[] xml);

    public MarcXBlock makeMarcXBlock() {
        return new MarcXBlock(this);
    }
}
