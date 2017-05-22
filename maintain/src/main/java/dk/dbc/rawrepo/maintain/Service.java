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

import dk.dbc.rawrepo.maintain.transport.ResponseError;
import dk.dbc.rawrepo.maintain.transport.C;
import com.sun.xml.ws.developer.SchemaValidation;
import dk.dbc.commons.webservice.WsdlValidationErrorHandler;
import dk.dbc.eeconfig.EEConfig;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.maintain.transport.PageContentResponse;
import dk.dbc.rawrepo.maintain.transport.RecordIds;
import dk.dbc.rawrepo.maintain.transport.TS;
import dk.dbc.rawrepo.maintain.transport.ValueEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;
import javax.xml.ws.Action;
import javax.xml.ws.Holder;
import javax.xml.ws.RequestWrapper;
import javax.xml.ws.ResponseWrapper;
import javax.xml.ws.WebServiceContext;
import javax.xml.ws.handler.MessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@WebService(targetNamespace = C.NS, serviceName = C.SERVICE, portName = C.PORT)
@SchemaValidation(handler = WsdlValidationErrorHandler.class)
public class Service {

    private static final Logger log = LoggerFactory.getLogger(Service.class);

    @Resource
    WebServiceContext webServiceContext;

    @Resource(lookup = C.DATASOURCE)
    DataSource rawrepo;

    @Inject
    @EEConfig.Url
    String openAgencyUrl;

    @Inject
    @EEConfig.Name(C.NAME)
    String name;

    ExecutorService executorService;

    OpenAgencyServiceFromURL openAgency = null;
    ResponseErrorException openAgencyError = null;

    @PostConstruct
    public void init() {
        log.info("init()");
        if (openAgencyUrl != null) {
            this.openAgency = OpenAgencyServiceFromURL.builder().build(openAgencyUrl);
        } else {
            log.error("openAgencyUrl is not defined");
            openAgencyError = new ResponseErrorException("Misconfiguration of server", ResponseError.Type.INTERNAL_SERVER_ERROR);
        }
        this.executorService = Executors.newFixedThreadPool(2);
    }

    @WebMethod(operationName = C.OPERATION_PAGE_CONTENT)
    @RequestWrapper(targetNamespace = C.NS, localName = C.OPERATION_PAGE_CONTENT + "Request", className = "dk.dbc.rawrepo.maintain.transport.PageContentRequest")
    @ResponseWrapper(targetNamespace = C.NS, localName = C.OPERATION_PAGE_CONTENT + "Response", className = "dk.dbc.rawrepo.maintain.transport.PageContentResponse")
    @Action(input = C.ACTION_PATH + C.OPERATION_PAGE_CONTENT + "Request", output = C.ACTION_PATH + C.OPERATION_PAGE_CONTENT + "Response")
    public void pageContent(@WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "method") String method,
                            @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "values") ArrayList<ValueEntry> values,
                            @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "leaving") String leaving,
                            @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.INOUT, name = "trackingId") Holder<String> trackingId,
                            @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "result") Holder<Object> out) {
        try {
            validateInput();

            HashMap<String, List<String>> valuesSet = new HashMap<>();
            if (values != null) {
                for (ValueEntry entry : values) {
                    valuesSet.put(entry.key, entry.value);
                }
            }
            log.debug("Remote IP: " + getIp() +
                      "; module = " + method +
                      "; map = " + valuesSet +
                      "; leaving = " + leaving +
                      "; trackingId = " + trackingId.value);

            HashMap<String, ArrayList<String>> valuesOut;

            switch (method) {
                case "queueRecords":
                    try (QueueRecords queueRecords = new QueueRecords(rawrepo, getOpenAgency(), executorService)) {
                        valuesOut = queueRecords.getValues(valuesSet, leaving);
                    }
                    break;
                case "removeRecords":
                    try (RemoveRecords removeRecords = new RemoveRecords(rawrepo, getOpenAgency(), executorService)) {
                        valuesOut = removeRecords.getValues(valuesSet, leaving);
                    }
                    break;
                case "revertRecords":
                    try (RevertRecords revertRecords = new RevertRecords(rawrepo, getOpenAgency(), executorService)) {
                        valuesOut = revertRecords.getValues(valuesSet, leaving);
                    }
                    break;
                case "showInfo":
                    valuesOut = new HashMap<>();
                    ArrayList<String> nameList = new ArrayList<>();
                    nameList.add(name);
                    valuesOut.put("name", nameList);
                    break;
                default:
                    throw new ResponseErrorException("Unknown module", ResponseError.Type.REQUEST_CONTENT_ERROR);
            }

            ArrayList<ValueEntry> outList = new ArrayList<>();
            for (Map.Entry<String, ArrayList<String>> entry : valuesOut.entrySet()) {
                ArrayList<String> valueList = entry.getValue();
                if (!valueList.isEmpty()) {
                    outList.add(new ValueEntry(entry.getKey(), valueList));
                }
            }
            out.value = new PageContentResponse.Result(outList);
        } catch (ResponseErrorException ex) {
            out.value = ex.getError();
        } catch (Exception ex) {
            log.error("Error: " + ex);
            log.info("Error: ", ex);
            out.value = new ResponseError("Server Error", ResponseError.Type.INTERNAL_SERVER_ERROR);
        }
    }

    @WebMethod(operationName = C.OPERATION_QUEUE_RECORDS)
    @RequestWrapper(targetNamespace = C.NS, localName = C.OPERATION_QUEUE_RECORDS + "Request", className = "dk.dbc.rawrepo.maintain.transport.QueueRecordsRequest")
    @ResponseWrapper(targetNamespace = C.NS, localName = C.OPERATION_QUEUE_RECORDS + "Response", className = "dk.dbc.rawrepo.maintain.transport.StandardResponse")
    @Action(input = C.ACTION_PATH + C.OPERATION_QUEUE_RECORDS + "Request", output = C.ACTION_PATH + C.OPERATION_QUEUE_RECORDS + "Response")
    public void queueRecords(@WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "agencyId") Integer agencyId,
                             @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "ids") RecordIds ids,
                             @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "provider") String provider,
                             @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.INOUT, name = "trackingId") Holder<String> trackingId,
                             @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "result") Holder<Object> out,
                             @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "timestamp") Holder<TS> outTimestamp) {
        try {
            log.debug("Remote IP: " + getIp());
            validateInput();
            try (QueueRecords queueRecords = new QueueRecords(rawrepo, getOpenAgency(), executorService)) {
                out.value = queueRecords.queueRecords(agencyId, ids.list, provider, trackingId.value);
            }
        } catch (ResponseErrorException ex) {
            out.value = ex.getError();
        } catch (Exception ex) {
            log.error("Error: " + ex);
            log.info("Error: ", ex);
            out.value = new ResponseError("Server Error", ResponseError.Type.INTERNAL_SERVER_ERROR);
        }
    }

    @WebMethod(operationName = C.OPERATION_REMOVE_RECORDS)
    @RequestWrapper(targetNamespace = C.NS, localName = C.OPERATION_REMOVE_RECORDS + "Request", className = "dk.dbc.rawrepo.maintain.transport.RemoveRecordsRequest")
    @ResponseWrapper(targetNamespace = C.NS, localName = C.OPERATION_REMOVE_RECORDS + "Response", className = "dk.dbc.rawrepo.maintain.transport.StandardResponse")
    @Action(input = C.ACTION_PATH + C.OPERATION_REMOVE_RECORDS + "Request", output = C.ACTION_PATH + C.OPERATION_REMOVE_RECORDS + "Response")
    public void removeRecords(@WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "agencyId") Integer agencyId,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "ids") RecordIds ids,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "provider") String provider,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.INOUT, name = "trackingId") Holder<String> trackingId,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "result") Holder<Object> out,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "timestamp") Holder<TS> outTimestamp) {
        try {
            log.debug("Remote IP: " + getIp());
            validateInput();
            try (RemoveRecords removeRecords = new RemoveRecords(rawrepo, getOpenAgency(), executorService)) {
                out.value = removeRecords.removeRecords(agencyId, ids.list, provider, trackingId.value);
            }
        } catch (ResponseErrorException ex) {
            out.value = ex.getError();
        } catch (Exception ex) {
            log.error("Error: " + ex);
            log.info("Error: ", ex);
            out.value = new ResponseError("Server Error", ResponseError.Type.INTERNAL_SERVER_ERROR);
        }
    }

    @WebMethod(operationName = C.OPERATION_REVERT_RECORDS)
    @RequestWrapper(targetNamespace = C.NS, localName = C.OPERATION_REVERT_RECORDS + "Request", className = "dk.dbc.rawrepo.maintain.transport.RevertRecordsRequest")
    @ResponseWrapper(targetNamespace = C.NS, localName = C.OPERATION_REVERT_RECORDS + "Response", className = "dk.dbc.rawrepo.maintain.transport.StandardResponse")
    @Action(input = C.ACTION_PATH + C.OPERATION_REVERT_RECORDS + "Request", output = C.ACTION_PATH + C.OPERATION_REVERT_RECORDS + "Response")
    public void revertRecords(@WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "agencyId") Integer agencyId,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "ids") RecordIds ids,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "time") TS time,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.IN, name = "provider") String provider,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.INOUT, name = "trackingId") Holder<String> trackingId,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "result") Holder<Object> out,
                              @WebParam(targetNamespace = C.NS, mode = WebParam.Mode.OUT, name = "timestamp") Holder<TS> outTimestamp) {
        try {
            log.debug("Remote IP: " + getIp());
            validateInput();
            try (RevertRecords revertRecords = new RevertRecords(rawrepo, getOpenAgency(), executorService)) {
                out.value = revertRecords.revertRecords(agencyId, ids.list, time.getMillis(), provider, trackingId.value);
            }
        } catch (ResponseErrorException ex) {
            out.value = ex.getError();
        } catch (Exception ex) {
            log.error("Error: " + ex);
            log.info("Error: ", ex);
            out.value = new ResponseError("Server Error", ResponseError.Type.INTERNAL_SERVER_ERROR);
        }
    }

    /*
     *       __  __     __
     *      / / / /__  / /___  ___  __________
     *     / /_/ / _ \/ / __ \/ _ \/ ___/ ___/
     *    / __  /  __/ / /_/ /  __/ /  (__  )
     *   /_/ /_/\___/_/ .___/\___/_/  /____/
     *               /_/
     */
    private void validateInput() throws ResponseErrorException {
        SAXException exception = WsdlValidationErrorHandler.fetchSAXParseException(webServiceContext);
        if (exception != null) {
            throw new ResponseErrorException(exception.getMessage(), ResponseError.Type.REQUEST_CONTENT_ERROR);
        }
    }

    private String getIp() {
        MessageContext messageContext = webServiceContext.getMessageContext();
        HttpServletRequest httpServletRequest = (HttpServletRequest) messageContext.get(MessageContext.SERVLET_REQUEST);
        return httpServletRequest.getRemoteAddr();
    }

    OpenAgencyServiceFromURL getOpenAgency() throws ResponseErrorException {
        if (openAgencyError != null) {
            throw openAgencyError;
        }
        return openAgency;
    }

    private static class ResponseErrorException extends Exception {

        private static final long serialVersionUID = 2415958974972575922L;

        private final String message;
        private final ResponseError.Type type;

        public ResponseErrorException(String message, ResponseError.Type type) {
            super(message);
            this.message = message;
            this.type = type;
        }

        ResponseError getError() {
            return new ResponseError(message, type);
        }
    }
}
