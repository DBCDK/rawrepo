/*
 * dbc-rawrepo-agency-delete
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-delete.
 *
 * dbc-rawrepo-agency-delete is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-delete is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-delete.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencypurge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
class AgencyPurge {

    private static final Logger log = LoggerFactory.getLogger(AgencyPurge.class);

    private final int agencyid;
    private final Connection connection;

    public AgencyPurge(String db, int agencyid) throws Exception {
        this.agencyid = agencyid;
        this.connection = getConnection(db);
    }

    private AgencyPurge() throws Exception {
        this.agencyid = 0;
        this.connection = null;
    }

    static AgencyPurge unittestObject() throws Exception {
        return new AgencyPurge();
    }


    public int countRecordsNotDeleted() throws SQLException {
        try(PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM records WHERE agencyid=? AND NOT DELETED")) {
            stmt.setInt(1, agencyid);
            try(ResultSet resultSet = stmt.executeQuery()) {
                if(resultSet.next())
                    return resultSet.getInt(1);
                throw new SQLException("No rows in query");
            }
        }
    }

    public int countQueueEntries() throws SQLException {
        try(PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM queue WHERE agencyid=?")) {
            stmt.setInt(1, agencyid);
            try(ResultSet resultSet = stmt.executeQuery()) {
                if(resultSet.next())
                    return resultSet.getInt(1);
                throw new SQLException("No rows in query");
            }
        }
    }

    public int purgeAgency() throws SQLException {
        try(PreparedStatement stmt = connection.prepareStatement("DELETE FROM records WHERE agencyid=?")) {
            stmt.setInt(1, agencyid);
            return stmt.executeUpdate();
        }
    }



    public void begin() throws SQLException {
        connection.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    public void rollback() throws SQLException {
        connection.rollback();
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;

    private static Connection getConnection(String url) throws SQLException {
        Matcher matcher = urlPattern.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(url + " Is not a valid jdbc uri");
        }
        Properties properties = new Properties();
        String jdbc = matcher.group(urlPatternPrefix);
        if (jdbc == null) {
            jdbc = jdbcDefault;
        }
        if (matcher.group(urlPatternUser) != null) {
            properties.setProperty("user", matcher.group(urlPatternUser));
        }
        if (matcher.group(urlPatternPassword) != null) {
            properties.setProperty("password", matcher.group(urlPatternPassword));
        }

        log.debug("Connecting");
        Connection connection = DriverManager.getConnection(jdbc + matcher.group(urlPatternHostPortDb), properties);
        log.debug("Connected");
        return connection;
    }

}
