/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-delete
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
class AgencyDelete {

    private static final Logger log = LoggerFactory.getLogger(AgencyDelete.class);

    private static final String AGENCYID = "agencyid";
    private static final String BIBLIOGRAPHICRECORDID = "bibliographicrecordid";

    private final int agencyid;
    private final Connection connection;
    private final RawRepoDAO dao;

    public AgencyDelete(String db, int agencyid) throws SQLException, RawRepoException {
        this.agencyid = agencyid;
        this.connection = getConnection(db);
        this.dao = RawRepoDAO.newInstance(connection);

    }

    public Set<String> getIds() throws SQLException {
        Set<String> set = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM records WHERE deleted = false AND agencyid = ?")) {
            stmt.setInt(1, agencyid);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    set.add(resultSet.getString(1));
                }
            }
        }
        return set;
    }

    public Set<String> getParentRelations() throws SQLException {
        Set<String> set = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM relations WHERE agencyid = refer_agencyid")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    set.add(resultSet.getString(1));
                }
            }
        }
        return set;
    }

    public Set<String> getSiblingRelations() throws SQLException {
        Set<String> set = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM relations WHERE refer_agencyid = ? AND agencyid <> refer_agencyid")) {
            stmt.setInt(1, agencyid);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    set.add(resultSet.getString(1));
                }
            }
        }
        return set;
    }

    void removeRelations() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement("SELECT refer_bibliographicrecordid, agencyid, bibliographicrecordid FROM relations WHERE refer_agencyid = ? AND agencyid <> refer_agencyid")) {
            stmt.setInt(1, agencyid);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    do {
                        log.error("Relation to me (id:" + resultSet.getString(1) + ") from (agency:" + resultSet.getInt(2) + "; id:" + resultSet.getString(3) + ")");
                    } while (resultSet.next());
                    throw new IllegalStateException("Relations to this agency");
                }
            }
        }

        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM relations WHERE agencyid = ?")) {
            stmt.setInt(1, agencyid);
            stmt.execute();
        }
    }

    void deleteRecords(Set<String> ids, Set<String> parentRelations, String role) throws RawRepoException, IOException, SQLException {
        int no = 0;

        DataTemplate template = new DataTemplate("content.xml");
        Properties props = new Properties();
        props.put(AGENCYID, String.valueOf(agencyid));
        try (PreparedStatement stmt = connection.prepareCall("{CALL enqueue(?, ?, ?, ?, ?, ?)}")) {
            stmt.setInt(2, agencyid);
            stmt.setString(4, role);
            stmt.setString(5, "Y");

            for (String id : ids) {
                log.debug("Processing bibliographicrecordid:" + id);
                props.put(BIBLIOGRAPHICRECORDID, id);
                Record record = dao.fetchRecord(id, agencyid);
                String mimeType = record.getMimeType();
                switch (mimeType) {
                    case MarcXChangeMimeType.DECENTRAL:
                    case MarcXChangeMimeType.AUTHORITTY:
                        break;
                    default:
                        mimeType = MarcXChangeMimeType.MARCXCHANGE;
                }

                record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
                record.setContent(template.build(props).getBytes("UTF-8"));
                record.setDeleted(true);
                dao.saveRecord(record);

                stmt.setString(1, id);
                stmt.setString(3, mimeType);
                stmt.setString(6, parentRelations.contains(id) ? "N" : "Y");
                stmt.execute();
                if (++no % 1000 == 0) {
                    System.err.print('·');
                }
            }
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
