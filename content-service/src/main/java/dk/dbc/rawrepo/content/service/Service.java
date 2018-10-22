/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import dk.dbc.eeconfig.EEConfig;
import dk.dbc.forsrights.client.ForsRightsException;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RawRepoExceptionRecordNotFound;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import dk.dbc.rawrepo.content.service.transport.FetchRequestAuthentication;
import dk.dbc.rawrepo.content.service.transport.FetchRequestRecord;
import dk.dbc.rawrepo.content.service.transport.FetchResponseError;
import dk.dbc.rawrepo.content.service.transport.FetchResponseRecord;
import dk.dbc.rawrepo.content.service.transport.FetchResponseRecordContent;
import dk.dbc.rawrepo.content.service.transport.FetchResponseRecords;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;
import org.xml.sax.SAXParseException;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.sql.DataSource;
import javax.xml.ws.Action;
import javax.xml.ws.Holder;
import javax.xml.ws.RequestWrapper;
import javax.xml.ws.ResponseWrapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public abstract class Service {
    private static final XLogger logger = XLoggerFactory.getXLogger(Service.class);

    @Resource(lookup = C.RAWREPO.DATASOURCE)
    DataSource rawrepo;

    @Inject
    AuthenticationService forsRights;

    @Inject
    OpenAgencyEJB openAgency;

    @Inject
    MarcXMergerEJB marcXMerger;

    @Inject
    XmlToolsEJB xmlTools;

    @Inject
    Timer requests;

    @Inject
    Counter requestErrors;

    @Inject
    Counter authorizationDenied;

    @Inject
    Counter requestSyntaxError;

    @Inject
    Timer authenticationRequest;

    @Inject
    Counter authenticationErrors;

    @Inject
    Timer fetchRaw;

    @Inject
    Timer fetchMerged;

    @Inject
    Timer fetchMergedDBCKat;

    @Inject
    Timer fetchCollection;

    @Inject
    @EEConfig.Name(C.X_FORWARDED_FOR)
    @EEConfig.Default("")
    String xForwardedFor;

    List<IPRange> ipRanges;

    ExecutorService executorService;

    @PostConstruct
    public void init() {
        logger.info("init()");
        ipRanges = new ArrayList<>();
        if (!xForwardedFor.isEmpty()) {
            for (String range : xForwardedFor.split("[,;:\\s]+")) {
                try {
                    ipRanges.add(new IPRange(range));
                } catch (RuntimeException ex) {
                    logger.warn("ipRange init - " + ex.getMessage());
                    logger.debug(ex.getMessage(), ex);
                }
            }
        }
        executorService = Executors.newFixedThreadPool(2);
    }

    @WebMethod(operationName = C.OPERATION_FETCH)
    @RequestWrapper(targetNamespace = C.NS, localName = C.OPERATION_FETCH + "Request", className = "dk.dbc.rawrepo.content.service.transport.FetchRequest")
    @ResponseWrapper(targetNamespace = C.NS, localName = C.OPERATION_FETCH + "Response", className = "dk.dbc.rawrepo.content.service.transport.FetchResponse")
    @Action(input = C.OPERATION_FETCH + "Request", output = C.OPERATION_FETCH + "Response")
    public void fetch(@WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "authentication") FetchRequestAuthentication authentication,
                      @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "records") List<FetchRequestRecord> requestRecords,
                      @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "error") Holder<Object> out) {
        try (Timer.Context time1 = requests.time()) {
            logger.debug("fetch()");
            Exception exception = getException();
            if (exception != null) {
                requestSyntaxError.inc();
                throw new ErrorException(exception.getMessage(), FetchResponseError.Type.REQUEST_CONTENT_ERROR);
            }

            boolean authenticated;
            try (Timer.Context time2 = authenticationRequest.time()) {
                if (authentication == null) {
                    authenticated = forsRights.validate(getClientIP());
                } else {
                    authenticated = forsRights.validate(authentication);
                }
            }
            if (!authenticated) {
                authorizationDenied.inc();
                throw new ErrorException("Authentication denied", FetchResponseError.Type.AUTHENTICATION_DENIED);
            }

            try (Connection connection = rawrepo.getConnection()) {
                RawRepoDAO dao = RawRepoDAO.builder(connection).relationHints(new RelationHintsOpenAgency(openAgency.getOpenAgencyService())).build();

                FetchResponseRecords fetchResponseRecords = new FetchResponseRecords();

                for (FetchRequestRecord requestRecord : requestRecords) {
                    boolean allowDeleted = requestRecord.allowDeleted == null ? false : requestRecord.allowDeleted;

                    logger.info("Request for id : " + requestRecord.bibliographicRecordId + " agency : " + requestRecord.agencyId +
                            " Delete allowed : " + allowDeleted + " mode " + requestRecord.mode.name() + " private " + (requestRecord.includeAgencyPrivate == null ? false : requestRecord.includeAgencyPrivate));
                    FetchResponseRecord record = new FetchResponseRecord(requestRecord.bibliographicRecordId, requestRecord.agencyId);
                    try {
                        switch (requestRecord.mode) {
                            case RAW:
                                if (allowDeleted ?
                                        !dao.recordExistsMaybeDeleted(requestRecord.bibliographicRecordId, requestRecord.agencyId) :
                                        !dao.recordExists(requestRecord.bibliographicRecordId, requestRecord.agencyId)) {
                                    throw new RawRepoExceptionRecordNotFound();
                                }
                                record.content = fetchRaw(dao, requestRecord);
                                break;
                            case MERGED:
                                record.content = fetchMerged(dao, requestRecord);
                                break;
                            case MERGED_DBCKAT:
                                record.content = fetchMergedDBCKat(dao, requestRecord);
                                break;
                            case COLLECTION:
                                record.content = fetchCollection(dao, requestRecord);
                                break;
                        }
                    } catch (RawRepoExceptionRecordNotFound ex) {
                        logger.warn("No such record: " + requestRecord.bibliographicRecordId + ";" + requestRecord.agencyId + ";" + requestRecord.mode);
                        record.content = "No such record";
                    }
                    fetchResponseRecords.records.add(record);
                    logger.info("Response added for id : " + requestRecord.bibliographicRecordId + " agency : " + requestRecord.agencyId +
                            " Delete allowed : " + allowDeleted + " mode " + requestRecord.mode.name() + " private " + (requestRecord.includeAgencyPrivate == null ? false : requestRecord.includeAgencyPrivate));
                }
                out.value = fetchResponseRecords;
            }
        } catch (MarcXMergerException ex) {
            requestErrors.inc();
            logger.error("MarcXMergerException Error: " + ex.getMessage());
            logger.debug("MarcXMergerException Error", ex);
            out.value = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (RawRepoException ex) {
            requestErrors.inc();
            logger.error("RawRepo Error: " + ex.getMessage());
            logger.debug("RawRepo Error", ex);
            out.value = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (SQLException ex) {
            requestErrors.inc();
            logger.error("SQL Error: " + ex.getMessage());
            logger.debug("SQL Error", ex);
            out.value = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (ForsRightsException ex) {
            authenticationErrors.inc();
            logger.error("Error Validating User: " + ex.getMessage());
            logger.debug("Error Validating User", ex);
            out.value = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        } catch (ErrorException e) {
            requestErrors.inc();
            out.value = e.getError();
        } catch (RuntimeException ex) {
            requestErrors.inc();
            logger.error("Runtime Exception: " + ex.getMessage());
            logger.debug("Runtime Exception", ex);
            logger.debug("Runtime Exception", ex.getCause());
            out.value = new FetchResponseError("Internal Server Error", FetchResponseError.Type.INTERNAL_SERVER_ERROR);
        }
    }

    private static final String NUM = "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])";
    private static final String IP = NUM + "\\." + NUM + "\\." + NUM + "\\." + NUM;
    private static final Pattern LAST_IP_PATTERN = Pattern.compile("\\b" + IP + "$", Pattern.DOTALL);

    @WebMethod(exclude = true)
    private String getClientIP() {
        String ip = getIp();
        String xForwardedFor = getXForwardedFor();
        if (xForwardedFor != null) {
            Matcher matcher = LAST_IP_PATTERN.matcher(xForwardedFor);
            if (matcher.find()) {
                xForwardedFor = matcher.group();
                for (IPRange ipRange : ipRanges) {
                    if (ipRange.inRange(ip)) {
                        return xForwardedFor;
                    }
                }
            }
        }
        return ip;
    }

    @WebMethod(exclude = true)
    public abstract SAXParseException getException();

    @WebMethod(exclude = true)
    public abstract String getIp();

    @WebMethod(exclude = true)
    public abstract String getXForwardedFor();

    /*
     *      ______     __       __
     *     / ____/__  / /______/ /_  ___  __________
     *    / /_  / _ \/ __/ ___/ __ \/ _ \/ ___/ ___/
     *   / __/ /  __/ /_/ /__/ / / /  __/ /  (__  )
     *  /_/    \___/\__/\___/_/ /_/\___/_/  /____/
     *
     */
    private FetchResponseRecordContent fetchRaw(RawRepoDAO dao, FetchRequestRecord requestRecord) throws RawRepoException {
        Record rawRecord;
        try (Timer.Context time = fetchRaw.time()) {
            rawRecord = dao.fetchRecord(requestRecord.bibliographicRecordId, requestRecord.agencyId);
        }
        byte[] content = rawRecord.getContent();
        if (isMarcXChange(rawRecord.getMimeType()) &&
                (requestRecord.includeAgencyPrivate == null || !requestRecord.includeAgencyPrivate)) {
            content = filterContent(content);
        }
        return new FetchResponseRecordContent(rawRecord.getMimeType(), content);
    }

    private FetchResponseRecordContent fetchMerged(RawRepoDAO dao, FetchRequestRecord requestRecord) throws RawRepoException, MarcXMergerException {
        Record rawRecord;
        try (Timer.Context time = fetchMerged.time();
             Pool.Element<MarcXMerger> marcXMergerElement = marcXMerger.take()) {
            boolean allowDeleted = requestRecord.allowDeleted == null ? false : requestRecord.allowDeleted;
            rawRecord = dao.fetchMergedRecordExpanded(requestRecord.bibliographicRecordId, requestRecord.agencyId, marcXMergerElement.getElement(), allowDeleted);
        } catch (RawRepoException | MarcXMergerException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        byte[] content = rawRecord.getContent();
        if (isMarcXChange(rawRecord.getMimeType()) &&
                (requestRecord.includeAgencyPrivate == null || !requestRecord.includeAgencyPrivate)) {
            content = filterContent(content);
        }
        return new FetchResponseRecordContent(rawRecord.getMimeType(), content);
    }

    private FetchResponseRecordContent fetchMergedDBCKat(RawRepoDAO dao, FetchRequestRecord requestRecord) throws RawRepoException, MarcXMergerException {
        logger.entry(requestRecord);
        Record rawRecord;

        try (Timer.Context time = fetchMergedDBCKat.time()) {
            String immutable = "001;010;020;990;991;996";
            String overwrite = "004;005;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

            FieldRules customFieldRules = new FieldRules(immutable, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);
            MarcXMerger merger = new MarcXMerger(customFieldRules, "CONTENT_SERVICE");

            boolean allowDeleted = requestRecord.allowDeleted == null ? false : requestRecord.allowDeleted;
            rawRecord = dao.fetchMergedRecord(requestRecord.bibliographicRecordId, requestRecord.agencyId, merger, allowDeleted);
        } catch (RawRepoException | MarcXMergerException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        byte[] content = rawRecord.getContent();
        if (isMarcXChange(rawRecord.getMimeType()) &&
                (requestRecord.includeAgencyPrivate == null || !requestRecord.includeAgencyPrivate)) {
            content = filterContent(content);
        }
        return new FetchResponseRecordContent(rawRecord.getMimeType(), content);
    }

    private Object fetchCollection(RawRepoDAO dao, FetchRequestRecord requestRecord) throws RawRepoException, MarcXMergerException {
        Map<String, Record> collection;
        try (Timer.Context time = fetchCollection.time();
             Pool.Element<MarcXMerger> marcXMergerElement = marcXMerger.take()) {
            if (requestRecord.allowDeleted &&
                    !dao.recordExists(requestRecord.bibliographicRecordId, requestRecord.agencyId) &&
                    dao.recordExistsMaybeDeleted(requestRecord.bibliographicRecordId, requestRecord.agencyId)) {
                Record rawRecord = dao.fetchRecord(requestRecord.bibliographicRecordId, requestRecord.agencyId);
                collection = new HashMap<>();
                collection.put(requestRecord.bibliographicRecordId, rawRecord);
            } else {
                collection = dao.fetchRecordCollectionExpanded(requestRecord.bibliographicRecordId, requestRecord.agencyId, marcXMergerElement.getElement());
            }
        } catch (RawRepoException | MarcXMergerException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        try (Pool.Element<XmlTools> xmlToolsElement = xmlTools.take()) {
            XmlTools.MarcXCollection combined = xmlToolsElement.getElement().buildCollection();

            for (Map.Entry<String, Record> entry : collection.entrySet()) {
                Record rawRecord = entry.getValue();

                if (!isMarcXChange(rawRecord.getMimeType())) {
                    return "Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType();
                }
                byte[] content = rawRecord.getContent();
                if (isMarcXChange(rawRecord.getMimeType()) &&
                        (requestRecord.includeAgencyPrivate == null || !requestRecord.includeAgencyPrivate)) {
                    content = filterContent(content);
                }
                combined.add(content);
            }
            byte[] combinedData = combined.build();
            return new FetchResponseRecordContent(MarcXChangeMimeType.MARCXCHANGE, combinedData);
        } catch (RawRepoException | MarcXMergerException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private byte[] filterContent(byte[] content) {
        try (Pool.Element<XmlTools> toolsElement = xmlTools.take()) {
            return toolsElement.getElement().filterPrivateOut(content);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static boolean isMarcXChange(String mimeType) {
        switch (mimeType) {
            case MarcXChangeMimeType.AUTHORITY:
            case MarcXChangeMimeType.ARTICLE:
            case MarcXChangeMimeType.ENRICHMENT:
            case MarcXChangeMimeType.MARCXCHANGE:
                return true;
            default:
                return false;
        }
    }

    /*
     *      __  ___
     *     /  |/  /__  ______________ _____ ____
     *    / /|_/ / _ \/ ___/ ___/ __ `/ __ `/ _ \
     *   / /  / /  __(__  |__  ) /_/ / /_/ /  __/
     *  /_/  /_/\___/____/____/\__,_/\__, /\___/
     *                              /____/
     *      ______                     __  _
     *     / ____/  __________  ____  / /_(_)___  ____
     *    / __/ | |/_/ ___/ _ \/ __ \/ __/ / __ \/ __ \
     *   / /____>  </ /__/  __/ /_/ / /_/ / /_/ / / / /
     *  /_____/_/|_|\___/\___/ .___/\__/_/\____/_/ /_/
     *                      /_/
     */
    private static class ErrorException extends Exception {

        private final FetchResponseError error;

        public ErrorException(String message, FetchResponseError.Type type) {
            error = new FetchResponseError(message, type);
        }

        public FetchResponseError getError() {
            return error;
        }
    }
}


