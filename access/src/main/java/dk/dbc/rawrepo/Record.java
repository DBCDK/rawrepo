/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import java.util.Date;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public interface Record {

    String MIMETYPE_MARCXCHANGE = "text/marcxchange";

    byte[] getContent();

    boolean isDeleted();

    void delete();

    void undelete();

    String getMimeType();

    void setMimeType(String mimeType);

    Date getCreated();

    RecordId getId();

    Date getModified();

    boolean isOriginal();

    void setContent(byte[] content);

    void setCreated(Date created);

    void setModified(Date modified);

}
