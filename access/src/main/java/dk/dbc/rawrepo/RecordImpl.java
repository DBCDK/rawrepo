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

import java.util.Arrays;
import java.util.Date;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class RecordImpl implements Record {

    RecordId id;
    byte[] content;
    Date created;
    Date modified;
    boolean original;

    RecordImpl(RecordId id) {
        this.id = id;
        this.content = null;
        this.created = new Date();
        this.modified = new Date();
        this.original = true;
    }

    /* for mocking */
    RecordImpl(String id, int library, byte[] content, Date created, Date modified, boolean original) {
        this.id = new RecordId(id, library);
        this.content = content;
        this.created = created;
        this.modified = modified;
        this.original = original;
    }

    @Override
    public RecordId getId() {
        return id;
    }

    @Override
    public byte[] getContent() {
        return Arrays.copyOf(content, content.length);
    }

    @Override
    public boolean hasContent() {
        return content != null;
    }

    @Override
    public void setContent(byte[] content) {
        this.modified = new Date();
        this.content = Arrays.copyOf(content, content.length);
    }

    @Override
    public Date getCreated() {
        return (Date) created.clone();
    }

    @Override
    public void setCreated(Date created) {
        if (!original) {
            throw new IllegalStateException("Cannot change creation time for objects from the database");
        }
        this.created = (Date) created.clone();
    }

    @Override
    public Date getModified() {
        return (Date) modified.clone();
    }

    @Override
    public void setModified(Date modified) {
        this.modified = (Date) modified.clone();
    }

    @Override
    public boolean isOriginal() {
        return original;
    }

    @Override
    public String toString() {
        return "Record{" + "id=" + id + ", content.length=" + (content != null ? content.length : "null") + ", created=" + created + ", modified=" + modified + ", original=" + original + '}';
    }

}
