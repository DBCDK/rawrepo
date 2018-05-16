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
package dk.dbc.rawrepo;

import java.time.Instant;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public interface RecordMetaData {

    RecordId getId();

    boolean isDeleted();

    void setDeleted(boolean deleted);

    String getMimeType();

    void setMimeType(String mimeType);

    Instant getCreated();

    void setCreated(Instant created);

    Instant getModified();

    void setModified(Instant modified);

    String getTrackingId();

    void setTrackingId(String trackingId);

}
