/*
 * dbc-rawrepo-queue-bulkload
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-queue-bulkload.
 *
 * dbc-rawrepo-queue-bulkload is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-queue-bulkload is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-queue-bulkload.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.queuebulkload;

import dk.dbc.rawrepo.RecordId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class LeafsCollection implements Iterable<RecordId>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LeafsCollection.class);

    private final HashSet<String> nodes;
    private final ResultSet query;
    private final int agencyId;

    public LeafsCollection(Connection connection, int agencyId) throws SQLException {
        nodes = new HashSet<>();
        this.agencyId = agencyId;

        try (PreparedStatement stmt = connection.prepareStatement("select distinct refer_bibliographicrecordid from relations where refer_bibliographicrecordid <> bibliographicrecordid")) {
            try (ResultSet nodesQuery = stmt.executeQuery()) {
                while (nodesQuery.next()) {
                    nodes.add(nodesQuery.getString(1));
                }
            }
        }

        PreparedStatement stmt = connection.prepareStatement("select bibliographicrecordid from records where agencyid = ?");
        try {
            stmt.setInt(1, agencyId);
            query = stmt.executeQuery();
        } catch (SQLException ex) {
            stmt.close();
            throw ex;
        }
    }

    @Override
    public Iterator<RecordId> iterator() {
        return new Iterator<RecordId>() {
            String next = null;
            boolean done = false;

            @Override
            public boolean hasNext() {
                if (!done) {
                    try {
                        while (next == null && query.next()) {
                            String tmp = query.getString(1);
                            if (!nodes.contains(tmp)) {
                                next = tmp;
                            }
                        }
                    } catch (SQLException ex) {
                        log.error("Caught: ", ex);
                    }
                    done = next == null;
                }
                return !done;
            }

            @Override
            public RecordId next() {
                RecordId recordId = new RecordId(next, agencyId);
                next = null;
                return recordId;
            }

            @Override
            public void remove() {
            }

        };
    }

    @Override
    public void close() throws Exception {
        Statement statement = query.getStatement();
        if (statement != null) {
            statement.close();
        }
    }

}
