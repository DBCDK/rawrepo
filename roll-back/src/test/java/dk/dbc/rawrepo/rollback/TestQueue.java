/*
 * Copyright (C) 2017 DBC A/S (http://dbc.dk/)
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
package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.QueueTarget;
import dk.dbc.rawrepo.RecordId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.jms.JMSException;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class TestQueue extends QueueTarget {

    private final ArrayList<String> queued;

    public TestQueue() {
        this.queued = new ArrayList<>();
    }

    @Override
    public void send(QueueJob job, List<String> queues) throws JMSException {
        RecordId id = job.getJob();
        for (String queue : queues) {
            queued.add(id.getBibliographicRecordId() + ":" + id.getAgencyId() + ":" + queue);
        }
    }

    public void clear() {
        queued.clear();
    }

    public int size() {
        return new HashSet<>(queued).size();
    }


    public void is(String... elems) {
        HashSet<String> missing = new HashSet();
        Collections.addAll(missing, elems);
        HashSet<String> extra = new HashSet(queued);
        extra.removeAll(missing);
        missing.removeAll(queued);
        if (!extra.isEmpty() || !missing.isEmpty()) {
            throw new RuntimeException("missing:" + missing.toString() + ", extra:" + extra.toString());
        }
    }

    @Override
    public String toString() {
        return queued.toString();
    }

    @Override
    public void commit() throws JMSException {
        queued.clear();
    }

}
