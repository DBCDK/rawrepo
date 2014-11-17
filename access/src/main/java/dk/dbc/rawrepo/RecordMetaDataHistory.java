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

import java.sql.Timestamp;
import java.util.Date;

public class RecordMetaDataHistory implements RecordMetaData {

    RecordId id;
    boolean deleted;
    String mimeType;
    Timestamp created;
    Timestamp modified;

    public RecordMetaDataHistory(RecordId id, boolean deleted, String mimeType, Timestamp created, Timestamp modified) {
        this.id = id;
        this.deleted = deleted;
        this.mimeType = mimeType;
        this.created = (Timestamp) created.clone();
        this.modified = (Timestamp) modified.clone();
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
    public Date getCreated() {
        return (Date) created.clone();
    }

    @Override
    public void setCreated(Date created) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Date getModified() {
        return (Date) modified.clone();
    }

    @Override
    public void setModified(Date modified) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Timestamp getTimestamp() {
        return (Timestamp) modified.clone();
    }

    @Override
    public String toString() {
        return "RecordMetaDataHistory{" + "id=" + id + ", deleted=" + deleted + ", mimeType=" + mimeType + ", created=" + created + ", modified=" + modified + '}';
    }

}