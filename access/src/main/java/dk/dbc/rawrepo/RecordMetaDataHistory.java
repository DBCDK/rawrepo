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

public class RecordMetaDataHistory implements RecordMetaData {

    RecordId id;
    boolean deleted;
    String mimeType;
    Instant created;
    Instant modified;
    String trackingId;

    public RecordMetaDataHistory(RecordId id, boolean deleted, String mimeType, Instant created, Instant modified, String trackingId) {
        this.id = id;
        this.deleted = deleted;
        this.mimeType = mimeType;
        this.created = Instant.from(created);
        this.modified = Instant.from(modified);
        this.trackingId = trackingId;
    }

    @Override
    public RecordId getId() {
        return id;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(boolean deleted) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMimeType() {
        return mimeType;
    }

    @Override
    public void setMimeType(String mimeType) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Instant getCreated() {
        return Instant.from(created);
    }

    @Override
    public void setCreated(Instant created) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Instant getModified() {
        return Instant.from(modified);
    }

    @Override
    public void setModified(Instant modified) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Instant getTimestamp() {
        return Instant.from(modified);
    }

    @Override
    public String getTrackingId() {
        return trackingId;
    }

    @Override
    public void setTrackingId(String trackingId) {
        this.trackingId = trackingId;
    }

    @Override
    public String toString() {
        return "RecordMetaDataHistory{" + "id=" + id + ", deleted=" + deleted + ", mimeType=" + mimeType + ", created=" + created + ", modified=" + modified + ", trackingId=" + trackingId + '}';
    }

}
