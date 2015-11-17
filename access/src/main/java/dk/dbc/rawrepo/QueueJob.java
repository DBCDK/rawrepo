/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import java.sql.Timestamp;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class QueueJob {

    RecordId job;
    String worker;
    Timestamp queued;

    QueueJob() {
    }

    QueueJob(String bibliographicRecordId, int agencyId, String worker, Timestamp queued) {
        this.job = new RecordId(bibliographicRecordId, agencyId);
        this.worker = worker;
        this.queued = queued;
    }

    public RecordId getJob() {
        return job;
    }

    public Timestamp getQueued() {
        return (Timestamp) queued.clone();
    }

    @Override
    public String toString() {
        return "QueueJob{" + "job=" + job + ", worker=" + worker + ", queued=" + queued + '}';
    }

}
