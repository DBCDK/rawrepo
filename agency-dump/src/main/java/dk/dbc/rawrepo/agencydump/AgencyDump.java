package dk.dbc.rawrepo.agencydump;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
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

import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnectorFactory;
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

    AgencyDump(String db, int agencyid, String vipCoreUrl) throws RawRepoException, SQLException {
        this.agencyid = agencyid;
        this.connection = getConnection(db);
        RawRepoDAO.Builder builder = RawRepoDAO.builder(connection);
        if(vipCoreUrl != null) {
            builder.relationHints(new RelationHintsVipCore(VipCoreLibraryRulesConnectorFactory.create(vipCoreUrl)));
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

    void dumpRecords(List<String> bibliographicRecordIds, OutputStream out, boolean merged) throws IOException, RawRepoException, MarcXMergerException, VipCoreException {
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
    private static final String JDBC_DEFAULT = "jdbc:postgresql://";
    private static final int URL_PATTERN_PREFIX = 1;
    private static final int URL_PATTERN_USER = 2;
    private static final int URL_PATTERN_PASSWORD = 3;
    private static final int URL_PATTERN_HOST_PORT_DB = 4;

    private static Connection getConnection(String url) throws SQLException {
        Matcher matcher = urlPattern.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(url + " Is not a valid jdbc uri");
        }
        Properties properties = new Properties();
        String jdbc = matcher.group(URL_PATTERN_PREFIX);
        if (jdbc == null) {
            jdbc = JDBC_DEFAULT;
        }
        if (matcher.group(URL_PATTERN_USER) != null) {
            properties.setProperty("user", matcher.group(URL_PATTERN_USER));
        }
        if (matcher.group(URL_PATTERN_PASSWORD) != null) {
            properties.setProperty("password", matcher.group(URL_PATTERN_PASSWORD));
        }

        log.debug("Connecting");
        Connection connection = DriverManager.getConnection(jdbc + matcher.group(URL_PATTERN_HOST_PORT_DB), properties);
        log.debug("Connected");
        return connection;
    }

}
