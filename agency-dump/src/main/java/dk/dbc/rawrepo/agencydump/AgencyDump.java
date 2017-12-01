/*
 * dbc-rawrepo-agency-dump
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-dump.
 *
 * dbc-rawrepo-agency-dump is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-dump is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-dump.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydump;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dk.dbc.rawrepo.RelationHintsOpenAgency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyDump implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AgencyDump.class);
    private final int agencyid;
    private final Connection connection;
    final RawRepoDAO dao;

    AgencyDump(String db, int agencyid, String openAgencyUrl) throws RawRepoException, SQLException {
        this.agencyid = agencyid;
        this.connection = getConnection(db);
        RawRepoDAO.Builder builder = RawRepoDAO.builder(connection);
        if(openAgencyUrl != null) {
            builder.relationHints(new RelationHintsOpenAgency(OpenAgencyServiceFromURL.builder().build(openAgencyUrl)));
        }
        this.dao = builder.build();
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException ex) {
            log.warn("Cannot close db connection");
        }
    }

    void dumpRecords(List<String> bibliographicRecordIds, OutputStream out, boolean merged) throws IOException, RawRepoException, MarcXMergerException {
        out.write("<?xml version=\"1.0\" encoding=\"utf8\"?>\n".getBytes(StandardCharsets.UTF_8));
        out.write("<marcx:collection xmlns:marcx=\"info:lc/xmlns/marcxchange-v1\">\n".getBytes(StandardCharsets.UTF_8));
        MarcXMerger marcXMerger = new MarcXMerger();
        int cnt = 0;
        for (String bibliographicRecordId : bibliographicRecordIds) {
            Record record;
            if (merged) {
                record = dao.fetchMergedRecord(bibliographicRecordId, agencyid, marcXMerger, false);
            } else {
                record = dao.fetchRecord(bibliographicRecordId, agencyid);
            }
            byte[] content = record.getContent();
            content = stripXML(content);
            out.write(content);
            out.write('\n');
            cnt++;
            if (cnt % 1000 == 0) {
                log.debug("Dumped {} records", cnt);
            }
        }
        out.write("</marcx:collection>\n".getBytes(StandardCharsets.UTF_8));
        log.info("Dumped all ({}) records", cnt);
    }

    private static final Pattern xml = Pattern.compile("<\\?[xX][mM][lL](\\s+\\w+=(?:'[^']*'|\"[^\"]*\"))*\\s*\\?>\\s*", Pattern.DOTALL);

    private byte[] stripXML(byte[] content) {
        String string = new String(content, StandardCharsets.UTF_8);
        Matcher matcher = xml.matcher(string);
        if (matcher.lookingAt()) {
            string = string.substring(matcher.end());
        }

        return string.getBytes(StandardCharsets.UTF_8);
    }

    public enum RecordCollection {

        ALL,
        ENRICHMENT,
        ENTITY
    }

    public List<String> getBibliographicRecordIds(RecordCollection type) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT bibliographicRecordId FROM records WHERE agencyId=").append(agencyid)
                .append(" AND NOT deleted");

        switch (type) {
            case ALL:
                break;
            case ENRICHMENT:
                sb.append(" AND bibliographicRecordId IN (SELECT bibliographicRecordId FROM relations where bibliographicRecordId=refer_bibliographicRecordId AND agencyId=").append(agencyid).append(")");
                break;
            case ENTITY:
                sb.append(" AND bibliographicRecordId NOT IN (SELECT bibliographicRecordId FROM relations where bibliographicRecordId=refer_bibliographicRecordId AND agencyId=").append(agencyid).append(")");
                break;
        }
        List<String> ret = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sb.toString())) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    ret.add(resultSet.getString(1));
                }
            }
        }
        log.info("Found {} records", ret.size());
        return ret;
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
