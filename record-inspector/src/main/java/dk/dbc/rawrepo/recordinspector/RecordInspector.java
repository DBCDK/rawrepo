/*
 * dbc-rawrepo-record-inspector
 * Copyright (C) 2014 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-record-inspector.
 *
 * dbc-rawrepo-record-inspector is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-record-inspector is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-record-inspector.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.recordinspector;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RecordInspector implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RecordInspector.class);
    private final RawRepoDAO dao;
    private Connection connection;

    public RecordInspector(String url, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) throws SQLException, RawRepoException {
        dao = openDatabase(url, vipCoreLibraryRulesConnector);
        connection.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void close() throws IOException {
        try {
            connection.rollback();
            connection.close();
        } catch (SQLException ex) {
            throw new IOException("Cannot close connection", ex);
        }
    }

    public static class RecordDescription {
        private final Timestamp timestamp;
        private final boolean deleted;
        private final String mimeType;

        public RecordDescription(Timestamp timestamp, boolean deleted, String mimeType) {
            this.timestamp = (Timestamp) timestamp.clone();
            this.deleted = deleted;
            this.mimeType = mimeType;
        }

        public Timestamp getTimestamp() {
            return (Timestamp) timestamp.clone();
        }

        @Override
        public String toString() {
            return timestamp + (deleted ? " deleted" : "") + " (" + mimeType + ')';
        }

    }

    public ArrayList<RecordDescription> timestamps(int agencyId, String bibliographicRecordId) throws SQLException {
        ArrayList<RecordDescription> ret = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT modified, deleted, mimetype FROM records WHERE agencyid=? AND bibliographicrecordid=?"
                + " UNION SELECT modified, deleted, mimetype FROM records_archive WHERE agencyid=? AND bibliographicrecordid=?"
                + " ORDER BY modified DESC")) {
            stmt.setInt(1, agencyId);
            stmt.setString(2, bibliographicRecordId);
            stmt.setInt(3, agencyId);
            stmt.setString(4, bibliographicRecordId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    Timestamp timestamp = resultSet.getTimestamp(1);
                    boolean deleted = resultSet.getBoolean(2);
                    String mimeType = resultSet.getString(3);
                    ret.add(new RecordDescription(timestamp, deleted, mimeType));
                }
            }
        }
        return ret;
    }

    public Record get(int agencyId, String bibliographicRecordId) throws RawRepoException, MarcXMergerException {
        return dao.fetchMergedRecord(bibliographicRecordId, agencyId, new MarcXMerger(), true);
    }

    public byte[] get(int agencyId, String bibliographicRecordId, Timestamp timestamp) throws SQLException {
        byte[] content = null;
        try (PreparedStatement stmt = connection.prepareStatement("SELECT content FROM records WHERE agencyid=? AND bibliographicrecordid=? AND modified=?"
                + " UNION SELECT content FROM records_archive WHERE agencyid=? AND bibliographicrecordid=? AND modified=?")) {
            stmt.setInt(1, agencyId);
            stmt.setString(2, bibliographicRecordId);
            stmt.setTimestamp(3, timestamp);
            stmt.setInt(4, agencyId);
            stmt.setString(5, bibliographicRecordId);
            stmt.setTimestamp(6, timestamp);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String base64Content = resultSet.getString(1);
                    content = base64Content == null ? null : DatatypeConverter.parseBase64Binary(base64Content);
                }
            }
        }
        return content;
    }

    public ArrayList<String> outboundRelations(int agencyId, String bibliographicRecordId) throws RawRepoException {
        Set<RecordId> relations = dao.getRelationsFrom(new RecordId(bibliographicRecordId, agencyId));
        ArrayList<String> ret = new ArrayList<>(relations.size());
        for (RecordId relation : relations) {
            ret.add(relation.getAgencyId() + ":" + relation.getBibliographicRecordId());
        }
        Collections.sort(ret);
        return ret;
    }

    public ArrayList<String> inboundRelations(int agencyId, String bibliographicRecordId) throws RawRepoException {
        Set<RecordId> relations = dao.getRelationsChildren(new RecordId(bibliographicRecordId, agencyId));
        ArrayList<String> ret = new ArrayList<>(relations.size());
        for (RecordId relation : relations) {
            ret.add(relation.getAgencyId() + ":" + relation.getBibliographicRecordId());
        }
        Collections.sort(ret);
        return ret;
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;

    private RawRepoDAO openDatabase(String url, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) throws SQLException, RawRepoException {
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
        connection = DriverManager.getConnection(jdbc + matcher.group(urlPatternHostPortDb), properties);
        log.debug("Connected");
        return RawRepoDAO.builder(connection).relationHints(new RelationHintsOpenAgency(vipCoreLibraryRulesConnector)).build();
    }

}
