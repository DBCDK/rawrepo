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

import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
class RecordImpl implements Record {

    RecordId id;
    boolean deleted;
    String mimeType;
    byte[] content;
    Date created;
    Date modified;
    String trackingId;
    boolean original;
    boolean enriched;
    String enrichmentTrail;

    RecordImpl(RecordId id) {
        this.id = id;
        this.deleted = false;
        this.mimeType = "";
        this.content = new byte[0];
        this.created = UTC();
        this.modified = UTC();
        this.trackingId = "";
        this.original = true;
        this.enriched = false;
        this.enrichmentTrail = String.valueOf(id.getAgencyId());
    }

    /* for mocking */
    RecordImpl(String id, int agencyId, boolean deleted, String mimeType, byte[] content, Date created, Date modified, String trackingId, boolean original) {
        this.id = new RecordId(id, agencyId);
        this.deleted = deleted;
        this.mimeType = mimeType;
        this.content = content;
        this.created = created;
        this.modified = modified;
        this.trackingId = trackingId;
        this.original = original;
        this.enriched = false;
        this.enrichmentTrail = String.valueOf(agencyId);
    }

    private RecordImpl(String id, int agencyId, String mimeType, byte[] content, Date created, Date modified, String trackingId, String enrichmentTrail) {
        this.id = new RecordId(id, agencyId);
        this.deleted = false;
        this.mimeType = mimeType;
        this.content = content;
        this.created = created;
        this.modified = modified;
        this.trackingId = trackingId;
        this.original = false;
        this.enriched = true;
        this.enrichmentTrail = enrichmentTrail;
    }

    static RecordImpl enriched(String id, int agencyId, String mimeType, byte[] content, Date created, Date modified, String trackingId, String enrichmentTrail) {
        return new RecordImpl(id, agencyId, mimeType, content, created, modified, trackingId, enrichmentTrail);
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
        this.modified = UTC();
        this.deleted = deleted;
    }

    @Override
    public byte[] getContent() {
        return Arrays.copyOf(content, content.length);
    }

    @Override
    public void setContent(byte[] content) {
        this.modified = UTC();
        this.content = Arrays.copyOf(content, content.length);
    }

    @Override
    public String getMimeType() {
        return mimeType;
    }

    @Override
    public void setMimeType(String mimeType) {
        this.modified = UTC();
        this.mimeType = mimeType;
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
    public String getTrackingId() {
        return trackingId;
    }

    @Override
    public void setTrackingId(String trackingId) {
        this.trackingId = trackingId;
    }

    @Override
    public boolean isOriginal() {
        return original;
    }

    @Override
    public boolean isEnriched() {
        return enriched;
    }

    @Override
    public void setEnriched(boolean enriched) {
        this.enriched = enriched;
    }

    @Override
    public String getEnrichmentTrail() {
        return enrichmentTrail;
    }

    @Override
    public String toString() {
        return "RecordImpl{" + "id=" + id + ", deleted=" + deleted + ", mimeType=" + mimeType + ", content=[...], created=" + created + ", modified=" + modified + ", original=" + original + ", enriched=" + enriched + ", trackingId=" + trackingId + ", enrichmentTrail=" + enrichmentTrail + '}';
    }

    private static Date UTC(Date date) {
        TimeZone tz = TimeZone.getDefault();
        int offset = tz.getRawOffset();
        if (tz.inDaylightTime(date)) {
            offset += tz.getDSTSavings();
        }
        return new Date(date.getTime() - offset);
    }

    private static Date UTC() {
        return UTC(new Date());
    }

}
