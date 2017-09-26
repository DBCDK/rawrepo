/*
 * dbc-rawrepo-introspect
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-introspect.
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-introspect.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.introsepct;

import dk.dbc.common.records.MarcConverter;
import dk.dbc.common.records.MarcRecord;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.*;
import dk.dbc.xmldiff.XmlDiff;
import dk.dbc.xmldiff.XmlDiffWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
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
    
    @Resource( name="env/InstanceName", lookup = "java:app/env/InstanceName")
    String InstanceName = "";

    @Inject
    Merger merger;

    @Inject
    JSONStreamer streamer;

    @Resource(lookup = "jdbc/rr")
    DataSource globalDataSource;

    @GET
    @Path("dbs")
    public Response getDBs() {
        if( InstanceName.equalsIgnoreCase("${ENV=INSTANCE_NAME}") ) {
            InstanceName="Missing InstanceName Environment";
        }

        ArrayList<String> response = new ArrayList<>();
        response.add(InstanceName);
        Collections.sort(response);
        return ok(response);
    }

    @GET
    @Path("agencies-with/{db : [^/]+}/{id : .+}")
    public Response getAgenciesWith(@PathParam("db") String resource,
                                    @PathParam("id") String bibliographicRecordId) {
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).build();
            Set<Integer> agencies = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);
            ArrayList<Integer> response = new ArrayList<>(agencies);
            Collections.sort(response);

            return ok(response);
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        }
    }

    private Record recordMergedFetcher( Integer agencyId, String bibliographicRecordId) throws XPathExpressionException, SAXException, IOException, RawRepoException, SQLException, MarcXMergerException {
        log.trace("Entering recordMergedFetcher");
        Record record = null;
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).searchOrder(new AgencySearchOrder(null) {
                @Override
                public List<Integer> provide(Integer key) throws Exception {
                    return Arrays.asList(key);
                }
            }).build();
            return record = dao.fetchMergedRecord(bibliographicRecordId, agencyId, merger.getMerger(), true);
        } finally {
            log.trace("Exit recordMergedFetcher : " + record );
        }
    }

    private Record recordFetcher ( Integer agencyId, String bibliographicRecordId) throws XPathExpressionException, SAXException, IOException, RawRepoException, SQLException, MarcXMergerException {
        log.trace("Entering recordFetcher");
        Record record = null;
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).searchOrder(new AgencySearchOrder(null) {
                @Override
                public List<Integer> provide(Integer key) throws Exception {
                    return Arrays.asList(key);
                }
            }).build();
            return record = dao.fetchRecord(bibliographicRecordId, agencyId);
        } finally {
            log.trace("Exit recordFetcher : " + record );
        }
    }


    @GET
    @Path("lineformatter/{db : [^/]+}/{agency : \\d+}/{id : .+}")
    public Response convertToLineFormat(@PathParam("db") String resource,
                                        @PathParam("agency") Integer agencyId,
                                        @PathParam("id") String bibliographicRecordId) {
        log.info("Enter -> convertToLineFormat");
        try {
            Record record = recordFetcher ( agencyId, bibliographicRecordId );
            MarcRecord mcr = MarcConverter.convertFromMarcXChange(new String(record.getContent(),  "UTF-8"));
            return ok(Arrays.asList(mcr.toString()));
        } catch (Exception ex) {
            log.error("Caught", ex);
            return fail("Internal Error " + ex);
        } finally {
            log.info("Exit -> convertToLineFormat : ");
        }
    }


    @GET
    @Path("record-merged/{db : [^/]+}/{agency : \\d+}/{id : .+}")
    public Response getRecordMerged(@PathParam("db") String resource,
                                    @PathParam("agency") Integer agencyId,
                                    @PathParam("id") String bibliographicRecordId) {
        try {
            Record record = recordMergedFetcher (agencyId, bibliographicRecordId );
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
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).build();
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
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).build();
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
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).build();
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
        try (Connection connection = globalDataSource.getConnection()) {
            RawRepoDAO dao = RawRepoDAO.builder(connection).build();
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
    private ArrayList<Object> xmlDiff(Record leftRecord, Record rightRecord) throws SAXException, IOException, XPathExpressionException {
        ByteArrayInputStream leftStream = new ByteArrayInputStream(leftRecord.getContent());
        ByteArrayInputStream rightStream = new ByteArrayInputStream(rightRecord.getContent());
        DiffWriter writer = new DiffWriter();
        XmlDiff.builder().indent(4).normalize(true).strip(true).trim(true).build()
                .compare(leftStream, rightStream, writer);
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

    private Response fail(String message) {
        return Response.serverError().entity(message).build();
    }

    private static class DiffWriter extends XmlDiffWriter {

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
