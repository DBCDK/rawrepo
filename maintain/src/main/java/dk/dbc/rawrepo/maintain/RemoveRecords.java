/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import dk.dbc.rawrepo.maintain.transport.StandardResponse.Result.Status;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC <dbc.dk>
 */
public class RemoveRecords extends RawRepoWorker {

    private static final Logger log = LoggerFactory.getLogger(RemoveRecords.class);

    private static final DataTemplate template = new DataTemplate("content.xml");

    public RemoveRecords(DataSource dataSource, OpenAgencyServiceFromURL openAgency, ExecutorService executorService) {
        super(dataSource, openAgency, executorService);
    }

    public HashMap<String, ArrayList<String>> getValues(HashMap<String, List<String>> valuesSet, String leaving) {
        HashMap<String, ArrayList<String>> values = new HashMap<>();

        try {
            values.put("provider", getProviders());
        } catch (SQLException ex) {
            log.warn("Sql error: " + ex.getMessage());
        }
        return values;
    }

    public Object removeRecords(Integer agencyId, List<String> ids, String provider, String trackingId) {
        log.debug("agencyId = " + agencyId +
                  "; ids = " + ids +
                  "; provider = " + provider +
                  "; trackingId = " + trackingId);
        ArrayList<StandardResponse.Result.Diag> diags = new ArrayList<>();
        int success = 0;
        int failed = 0;
        try {
            Connection connection = getConnection();

            for (String id : ids) {
                log.info("remove: " + id + ":" + agencyId);
                connection.setAutoCommit(false);
                try {
                    removeRecord(agencyId, id, provider, trackingId);
                    connection.commit();
                    success++;
                } catch (RawRepoException ex) {
                    failed++;
                    diags.add(new StandardResponse.Result.Diag("Record: " + id, ex.getMessage()));
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        log.warn("Record remove error: " + ex.getMessage());
                    }
                    if (!connection.getAutoCommit()) {
                        try {
                            connection.rollback();
                        } catch (SQLException ex1) {
                            log.warn("Cannot roll back " + ex1.getMessage());
                        }
                    }
                }
            }
            StandardResponse.Result.Status status = Status.SUCCESS;
            StringBuilder message = new StringBuilder();
            message.append("Done!");
            message.append("\n  * Successfully removed: ").append(success).append(" records.");
            if (failed != 0) {
                status = Status.PARTIAL;
                message.append("\n  * Failed to remove: ").append(failed).append(" records.");
            }

            return new StandardResponse.Result(status, message.toString(), diags);
        } catch (SQLException ex) {
            log.error("Error getting database connection: " + ex.getMessage());
            return new StandardResponse.Result(StandardResponse.Result.Status.FAILURE, "Error getting database connection");
        }

    }

    void removeRecord(Integer agencyId, String id, String provider, String trackingId) throws SQLException, RawRepoException {
        RawRepoDAO dao = getDao();
        Record record = dao.fetchRecord(id, agencyId);
        if (record.isOriginal()) {
            throw new RawRepoException("Record does not exist");
        }
        if (record.isDeleted()) {
            throw new RawRepoException("Record already deleted");
        }
        if (!dao.getRelationsChildren(record.getId()).isEmpty()) {
            throw new RawRepoException("There's relations to this record (has children)");
        }
        if (!dao.getRelationsSiblingsToMe(record.getId()).isEmpty()) {
            throw new RawRepoException("There's relations to this record (has siblings)");
        }
        dao.changedRecord(provider, record.getId(), record.getMimeType());
        dao.setRelationsFrom(record.getId(), new HashSet<RecordId>());
        record.setDeleted(true);
        if (record.getMimeType().equals(MarcXChangeMimeType.ENRICHMENT)) {
            record.setMimeType(MarcXChangeMimeType.MARCXCHANGE);
        }
        String content = template.build("agencyid", String.format("%06d", agencyId),
                                        "bibliographicrecordid", id);
        record.setContent(content.getBytes(StandardCharsets.UTF_8));
        if (trackingId != null) {
            record.setTrackingId(trackingId);
        }
        dao.saveRecord(record);
    }


    /*
     *     ______                     __      __
     *    /_  __/__  ____ ___  ____  / /___ _/ /____
     *     / / / _ \/ __ `__ \/ __ \/ / __ `/ __/ _ \
     *    / / /  __/ / / / / / /_/ / / /_/ / /_/  __/
     *   /_/  \___/_/ /_/ /_/ .___/_/\__,_/\__/\___/
     *                     /_/
     */
    private static class DataTemplate {

        private static final Pattern VARIABLE = Pattern.compile("\\$\\{([0-9a-z](?:[-.]?[0-9a-z])*)\\}");
        private ArrayList<String> list; // odd number entries are verbatim, even are properties

        public DataTemplate(String file) {

            try {
                InputStream stream = getClass().getClassLoader().getResourceAsStream(file);
                if (stream == null) {
                    throw new FileNotFoundException(file);
                }
                byte[] bytes = new byte[stream.available()];
                int read = stream.read(bytes);
                if (read != bytes.length) {
                    throw new IOException("Shortread expected " + bytes.length + " got " + read);
                }
                this.list = new ArrayList<>();
                String data = new String(bytes, "UTF-8");
                Matcher matcher = VARIABLE.matcher(data);
                int start = 0;
                while (matcher.find()) {
                    list.add(data.substring(start, matcher.start()));
                    list.add(matcher.group(1));
                    start = matcher.end();
                }
                list.add(data.substring(start));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public String build(Properties props) {
            return build(props, "[UNSET]");
        }

        public String build(Properties props, String defaultValue) {
            StringBuilder sb = new StringBuilder();
            for (Iterator<String> iterator = list.iterator() ; iterator.hasNext() ;) {
                sb.append(iterator.next());
                if (iterator.hasNext()) {
                    sb.append(props.getProperty(iterator.next(), defaultValue));
                }
            }
            return sb.toString();
        }

        public String build(String... s) {
            Properties props = new Properties();
            int i = 0;
            while (i < s.length - 1) {
                props.put(s[i], s[i + 1]);
                i = i + 2;
            }
            return build(props, i < s.length ? s[i] : "[UNSET]");
        }
    }
}
