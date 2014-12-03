/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-introspect
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.introsepct;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.xmldiff.XmlDiff;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
@Stateless
@Path("")
public class Introspect {

    private static final Logger log = LoggerFactory.getLogger(Introspect.class);

    private static final String KEY_BIBLIOGRAPHICRECORDID = "bibliographicrecordid";
    private static final String KEY_AGENCYID = "agencyid";
    private static final String KEY_DELETED = "deleted";
    private static final String KEY_MIMETYPE = "mimetype";
    private static final String KEY_MODIFIED = "modified";
    private static final String KEY_CREATED = "created";
    private static final String KEY_ME = "me";
    private static final String KEY_SIBLINGS_IN = "siblings-in";
    private static final String KEY_SIBLINGS_OUT = "siblings-out";
    private static final String KEY_PARENTS = "parents";
    private static final String KEY_CHILDREN = "children";

    Context context;

    @Resource(name = "jdbcResourceBase")
    String jdbcResourceBase;

    @Inject
    Merger merger;

    @Inject
    JSONStreamer streamer;

    /**
     * @param resource
     * @return the dataSource
     */
    DataSource getDataSource(String resource) {
        try {
            return (DataSource) context.lookup(jdbcResourceBase + "/" + resource);
        } catch (NamingException ex) {
            throw new RuntimeException("Cannot get container resource", ex);
        }
    }

    @PostConstruct
    public void postConstruct() {
        try {
            context = new InitialContext();
        } catch (NamingException ex) {
            throw new RuntimeException("Cannot get container resources", ex);
        }
    }

    @GET
    @Path("dbs")
    public Response getDBs() {
        try {
            ArrayList<String> response = new ArrayList<>();
            NamingEnumeration<NameClassPair> list = context.list(jdbcResourceBase);
            while (list.hasMoreElements()) {
                NameClassPair element = list.nextElement();
                if (element.isRelative() && element.getName().matches("^[-a-z0-9]+$")) {
                    Object obj = context.lookup(jdbcResourceBase + "/" + element.getName());
                    if (DataSource.class.isAssignableFrom(obj.getClass())) {
                        response.add(element.getName());
                    }
                }
            }
            Collections.sort(response);
            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("agencies-with/{db : [^/]+}/{id : .+}")
    public Response getAgenciesWith(@PathParam("db") String resource,
                                    @PathParam("id") String bibliographicRecordId) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            Set<Integer> agencies = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);
            ArrayList<Integer> response = new ArrayList<>(agencies);
            Collections.sort(response);

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("record-merged/{db : [^/]+}/{agency : \\d+}/{id : .+}")
    public Response getRecordMerged(@PathParam("db") String resource,
                                    @PathParam("agency") Integer agencyId,
                                    @PathParam("id") String bibliographicRecordId) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            Record record = dao.fetchMergedRecord(bibliographicRecordId, agencyId, merger.getMerger());

            ArrayList<Object> response = xmlDiff(record, record);

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("record-historic/{db : [^/]+}/{agency : \\d+}/{id : .+}/{version : \\d+}")
    public Response getRecordHistoric(@PathParam("db") String resource,
                                      @PathParam("agency") Integer agencyId,
                                      @PathParam("id") String bibliographicRecordId,
                                      @PathParam("version") Integer version) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(bibliographicRecordId, agencyId);
            Record record = dao.getHistoricRecord(recordHistory.get(version));

            ArrayList<Object> response = xmlDiff(record, record);

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("record-diff/{db : [^/]+}/{agency : \\d+}/{id : .+}/{left : \\d+}/{right : \\d+}")
    public Response getRecordDiff(@PathParam("db") String resource,
                                  @PathParam("agency") Integer agencyId,
                                  @PathParam("id") String bibliographicRecordId,
                                  @PathParam("left") Integer left,
                                  @PathParam("right") Integer right) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(bibliographicRecordId, agencyId);
            Record leftRecord = dao.getHistoricRecord(recordHistory.get(left));
            Record rightRecord = dao.getHistoricRecord(recordHistory.get(right));

            ArrayList<Object> response = xmlDiff(leftRecord, rightRecord);

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("record-history/{db : [^/]+}/{agency : \\d+}/{id : .+}")
    public Response getRecordHistory(@PathParam("db") String resource,
                                     @PathParam("agency") Integer agencyId,
                                     @PathParam("id") String bibliographicRecordId) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            List<RecordMetaDataHistory> recordHistory = dao.getRecordHistory(bibliographicRecordId, agencyId);
            ArrayList<Object> response = new ArrayList<>();
            for (RecordMetaDataHistory recordMetaData : recordHistory) {
                HashMap<String, Object> record = recordMetaDataObject(recordMetaData);

                response.add(record);
            }
            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    @GET
    @Path("relations/{db : [^/]+}/{agency : \\d+}/{id : .+}")
    public Response getRelations(@PathParam("db") String resource,
                                 @PathParam("agency") Integer agencyId,
                                 @PathParam("id") String bibliographicRecordId) {
        try (Connection connection = getDataSource(resource).getConnection()) {
            RawRepoDAO dao = RawRepoDAO.newInstance(connection);
            RecordId recordId = new RecordId(bibliographicRecordId, agencyId);
            HashMap<String, Object> response = new HashMap();

            response.put(KEY_ME, recordIdObject(recordId));
            response.put(KEY_CHILDREN, relationListObject(dao.getRelationsChildren(recordId)));
            response.put(KEY_PARENTS, relationListObject(dao.getRelationsParents(recordId)));
            response.put(KEY_SIBLINGS_OUT, relationListObject(dao.getRelationsSiblingsFromMe(recordId)));
            response.put(KEY_SIBLINGS_IN, relationListObject(dao.getRelationsSiblingsToMe(recordId)));

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    //  _   _      _                 _____                 _   _
    // | | | | ___| |_ __   ___ _ __|  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |  |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|  |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|
    //
    private ArrayList<Object> xmlDiff(Record leftRecord, Record rightRecord) throws SAXException, IOException {
        XmlDiff diff = new XmlDiff();
        diff.indent("    ");
        diff.strip(true);
        diff.trim(true);
        diff.unicodeNormalize(true);
        ByteArrayInputStream leftStream = new ByteArrayInputStream(leftRecord.getContent());
        ByteArrayInputStream rightStream = new ByteArrayInputStream(rightRecord.getContent());
        DiffWriter writer = new DiffWriter();
        diff.diff(leftStream, rightStream, writer);
        ArrayList<Object> response = writer.getData();
        return response;
    }

    private HashMap<String, Object> recordMetaDataObject(RecordMetaDataHistory recordMetaData) {
        HashMap<String, Object> record = new HashMap<>();
        record.put(KEY_AGENCYID, recordMetaData.getId().getAgencyId());
        record.put(KEY_BIBLIOGRAPHICRECORDID, recordMetaData.getId().getBibliographicRecordId());
        record.put(KEY_DELETED, recordMetaData.isDeleted());
        record.put(KEY_MIMETYPE, recordMetaData.getMimeType());
        record.put(KEY_CREATED, recordMetaData.getCreated());
        record.put(KEY_MODIFIED, recordMetaData.getModified());
        return record;
    }

    private HashMap<String, Object> recordIdObject(RecordId recordId) {
        HashMap<String, Object> record = new HashMap<>();
        record.put(KEY_AGENCYID, recordId.getAgencyId());
        record.put(KEY_BIBLIOGRAPHICRECORDID, recordId.getBibliographicRecordId());
        return record;
    }

    private ArrayList<Object> relationListObject(Set<RecordId> relations) {
        ArrayList<Object> list = new ArrayList<>();
        for (RecordId recordId : relations) {
            list.add(recordIdObject(recordId));
        }
        return list;
    }

    private Response ok(Object data) {
        return Response.ok(streamer.stream(data)).type(MediaType.APPLICATION_JSON_TYPE).build();
    }

    private static Response fail(String message) {
        return Response.serverError().entity(message).build();
    }

    private static class DiffWriter extends XmlDiff.Writer {

        private final ArrayList<Object> data;
        private StringBuilder sb;

        public DiffWriter() {
            this.data = new ArrayList<>();
            this.sb = new StringBuilder();

        }

        private void add(String type) {
            if (!sb.toString().isEmpty()) {
                HashMap<String, String> obj = new HashMap<>();
                //log.debug(type + ":" + sb.toString());
                obj.put("type", type);
                obj.put("content", sb.toString());
                sb = new StringBuilder();
                data.add(obj);
            }
        }

        @Override
        public void closeUri() {
            add("uri");
        }

        @Override
        public void openUri() {
            add("both");
        }

        @Override
        public void closeName() {
            add("name");
        }

        @Override
        public void openName() {
            add("both");
        }

        @Override
        public void closeRight() {
            add("right");
        }

        @Override
        public void openRight() {
            add("both");
        }

        @Override
        public void closeLeft() {
            add("left");
        }

        @Override
        public void openLeft() {
            add("both");
        }

        @Override
        public void write(String s) {
            sb.append(s);
        }

        public ArrayList<Object> getData() {
            add("both");
            return data;
        }

    }

}
