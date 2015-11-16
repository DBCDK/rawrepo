/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-record-load
 *
 * dbc-rawrepo-record-load is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-record-load is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.recordload;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
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
public class RecordLoad implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RecordLoad.class);
    private final RawRepoDAO dao;
    private Connection connection;

    public RecordLoad(String url) throws SQLException, RawRepoException {
        dao = openDatabase(url);
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

    public void save(int agencyId, String bibliographicRecordId, String mimeType, byte[] content) throws RawRepoException {
        Record record = dao.fetchRecord(bibliographicRecordId, agencyId);
        boolean original = record.isOriginal();
        if (original) {
            log.info("Newly created record");
        }
        record.setDeleted(false);
        record.setMimeType(mimeType);
        record.setContent(content);
        dao.saveRecord(record);
    }

    public void enqueue(int agencyId, String bibliographicRecordId, String role, String mimetype) throws RawRepoException {
        RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
        dao.changedRecord(role, recordId, mimetype);
    }

    public void delete(int agencyId, String bibliographicRecordId) throws RawRepoException {
        Record record = dao.fetchRecord(bibliographicRecordId, agencyId);
        record.setDeleted(true);
        record.setContent(( "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                            + "<marcx:record xmlns:marcx=\"info:lc/xmlns/marcxchange-v1\" format=\"danMARC2\" type=\"Bibliographic\">\n"
                            + "    <marcx:leader>00000d    2200000   4500</marcx:leader>\n"
                            + "    <marcx:datafield ind1=\"0\" ind2=\"0\" tag=\"001\">\n"
                            + "        <marcx:subfield code=\"a\">" + bibliographicRecordId + "</marcx:subfield>\n"
                            + "        <marcx:subfield code=\"b\">" + agencyId + "</marcx:subfield>\n"
                            + "    </marcx:datafield>\n"
                            + "    <marcx:datafield ind1=\"0\" ind2=\"0\" tag=\"004\">\n"
                            + "        <marcx:subfield code=\"r\">d</marcx:subfield>\n"
                            + "    </marcx:datafield>\n"
                            + "</marcx:record>" ).getBytes(StandardCharsets.UTF_8));
        record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        dao.deleteRelationsFrom(record.getId());
        dao.saveRecord(record);
    }

    private static final Pattern RELATION = Pattern.compile("^(\\d+):(.+)$");

    public void relations(int agencyId, String bibliographicRecordId, boolean add, List<String> relations) throws RawRepoException {
        Set<RecordId> relationSet;
        if (add) {
            relationSet = dao.getRelationsFrom(new RecordId(bibliographicRecordId, agencyId));
        } else {
            relationSet = new HashSet<>();
        }
        for (String rel : relations) {
            Matcher matcher = RELATION.matcher(rel);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Not a valid relation: " + rel);
            }
            int relAgencyId = Integer.parseInt(matcher.group(1), 10);
            String relBibliographicRecordId = matcher.group(2);
            relationSet.add(new RecordId(relBibliographicRecordId, relAgencyId));
        }
        dao.setRelationsFrom(new RecordId(bibliographicRecordId, agencyId), relationSet);
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String jdbcDefault = "jdbc:postgresql://";
    private static final int urlPatternPrefix = 1;
    private static final int urlPatternUser = 2;
    private static final int urlPatternPassword = 3;
    private static final int urlPatternHostPortDb = 4;

    private RawRepoDAO openDatabase(String url) throws SQLException, RawRepoException {
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
        return RawRepoDAO.builder(connection).build();
    }

}
