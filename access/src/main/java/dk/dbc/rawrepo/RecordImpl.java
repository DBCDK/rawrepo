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
import java.util.Arrays;

/**
 * @author DBC {@literal <dbc.dk>}
 */
class RecordImpl implements Record {

    RecordId id;
    boolean deleted;
    String mimeType;
    byte[] content;
    Instant created;
    Instant modified;
    String trackingId;
    boolean original;
    boolean enriched;
    String enrichmentTrail;

    RecordImpl(RecordId id) {
        this.id = id;
        this.deleted = false;
        this.mimeType = "";
        this.content = new byte[0];
        /*
            A few notes about Instant and time zones:

            Using Instant.now() relies on the JVM running in the correct time zone. In this case it works because the
            parent Payara image sets the time zone explicit with 'create-jvm-options -Duser.timezone=CET'
            If this value is ever changed or removed it will likely have adverse effect on updateservice - but the
            problem will be bigger then just updateservice.

            We need the time zone as the column in postgres is with time zone. Without setting the time zone in the Java
            layer the time will be treated as being UTC, which means one or two hours (depending on summer/winter time)
            will be subtracted from the date.
         */
        this.created = Instant.now();
        this.modified = Instant.now();
        this.trackingId = "";
        this.original = true;
        this.enriched = false;
        this.enrichmentTrail = String.valueOf(id.getAgencyId());
    }

    /* for mocking */
    RecordImpl(String id, int agencyId, boolean deleted, String mimeType, byte[] content, Instant created, Instant modified, String trackingId, boolean original) {
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

    private RecordImpl(String id, int agencyId, String mimeType, byte[] content, Instant created, Instant modified, String trackingId, String enrichmentTrail) {
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

    static RecordImpl enriched(String id, int agencyId, String mimeType, byte[] content, Instant created, Instant modified, String trackingId, String enrichmentTrail) {
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
        this.modified = Instant.now();
        this.deleted = deleted;
    }

    @Override
    public byte[] getContent() {
        return Arrays.copyOf(content, content.length);
    }

    @Override
    public void setContent(byte[] content) {
        this.modified = Instant.now();
        this.content = Arrays.copyOf(content, content.length);
    }

    @Override
    public String getMimeType() {
        return mimeType;
    }

    @Override
    public void setMimeType(String mimeType) {
        this.modified = Instant.now();
        this.mimeType = mimeType;
    }

    @Override
    public Instant getCreated() {
        return Instant.from(created);
    }

    @Override
    public void setCreated(Instant created) {
        if (!original) {
            throw new IllegalStateException("Cannot change creation time for objects from the database");
        }
        this.created = Instant.from(created);
    }

    @Override
    public Instant getModified() {
        return Instant.from(modified);
    }

    @Override
    public void setModified(Instant modified) {
        this.modified = Instant.from(modified);
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

}
