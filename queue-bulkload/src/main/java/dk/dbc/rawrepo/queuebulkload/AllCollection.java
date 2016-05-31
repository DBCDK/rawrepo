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
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC <dbc.dk>
 */
public class AllCollection implements Iterable<RecordId>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AllCollection.class);

    private final ResultSet resultSet;

    public AllCollection(Connection connection, int library) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid, agencyid FROM records WHERE agencyid=?");
        try {
            stmt.setInt(1, library);
            resultSet = stmt.executeQuery();
        } catch (Exception e) {
            stmt.close();
            throw e;
        }
    }

    AllCollection(Connection connection, Integer library, Timestamp from, Timestamp to) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid, agencyid FROM records WHERE agencyid=? AND modified >= ? AND modified <= ?");
        try {
            stmt.setInt(1, library);
            stmt.setTimestamp(2, from);
            stmt.setTimestamp(3, to);
            resultSet = stmt.executeQuery();
        } catch (Exception e) {
            stmt.close();
            throw e;
        }
    }

    @Override
    public Iterator<RecordId> iterator() {
        return new Iterator<RecordId>() {
            boolean hasNextCache = false;
            boolean resultsetNext = true;

            @Override
            public boolean hasNext() {
                try {
                    if (!hasNextCache) {
                        if (resultsetNext) {
                            hasNextCache = resultsetNext = resultSet.next();
                        }
                    }
                } catch (SQLException ex) {
                    hasNextCache = false;
                    throw new RuntimeException("Advance sql cursor: ", ex);
                }
                return hasNextCache;
            }

            @Override
            public RecordId next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNextCache = false;
                try {
                    String id = resultSet.getString(1);
                    int library = resultSet.getInt(2);
                    return new RecordId(id, library);
                } catch (SQLException ex) {
                    throw new IllegalStateException("Caught sql error", ex);
                }
            }

            @Override
            public void remove() {
                throw new IllegalStateException("Cannot remove things from resultset");
            }
        };
    }

    @Override
    public void close() throws Exception {
        log.debug("Close all");
        try (Statement stmt = resultSet.getStatement()) {
            resultSet.close();
        }
    }

}
