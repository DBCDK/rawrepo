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

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import dk.dbc.rawrepo.maintain.transport.StandardResponse.Result.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author DBC {@literal <dbc.dk>}
 */
class RemoveRecords extends RawRepoWorker {

    private static final Logger log = LoggerFactory.getLogger(RemoveRecords.class);

    private final DocumentBuilder documentBuilder;
    private final Transformer transformer;

    RemoveRecords(DataSource dataSource, OpenAgencyServiceFromURL openAgency, ExecutorService executorService) throws MarcXMergerException {
        super(dataSource, openAgency, executorService);
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
    }

    RemoveRecords(DataSource dataSource, OpenAgencyServiceFromURL openAgency) throws MarcXMergerException {
        super(dataSource, openAgency, null);
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
    }

    HashMap<String, ArrayList<String>> getValues(HashMap<String, List<String>> valuesSet, String leaving) {
        HashMap<String, ArrayList<String>> values = new HashMap<>();

        try {
            values.put("provider", getProviders());
        } catch (SQLException ex) {
            log.warn("Sql error: " + ex.getMessage());
        }
        return values;
    }

    Object removeRecords(Integer agencyId, List<String> ids, String provider, String trackingId) {
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
                } catch (RawRepoException | DOMException | IOException | SAXException | TransformerException ex) {
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

    void removeRecord(Integer agencyId, String id, String provider, String trackingId) throws RawRepoException, SAXException, TransformerException, DOMException, IOException {
        RawRepoDAO dao = getDao();

        int agencyIdFor = dao.agencyFor(id, agencyId, true);
        Record record = dao.fetchRecord(id, agencyIdFor);

        if (record.isDeleted()) {
            throw new RawRepoException("Record already deleted");
        }
        if (!dao.getRelationsChildren(record.getId()).isEmpty()) {
            throw new RawRepoException("There's relations to this record (has children)");
        }
        if (!dao.getRelationsSiblingsToMe(record.getId()).isEmpty()) {
            throw new RawRepoException("There's relations to this record (has siblings)");
        }

        dao.changedRecord(provider, record.getId());

        if (record.getId().getAgencyId() != agencyId) {
            log.debug("Creating record for: " + id);
            Record r = dao.fetchRecord(id, agencyId);
            r.setContent(record.getContent());
            r.setMimeType(record.getMimeType());
            record = r;
        }
        byte[] content = markMarcContentDeleted(record.getContent());
        record.setContent(content);
        record.setDeleted(true);
        if (trackingId != null) {
            record.setTrackingId(trackingId);
        }
        dao.deleteRelationsFrom(record.getId());
        dao.saveRecord(record);

    }

    private byte[] markMarcContentDeleted(byte[] content) throws SAXException, TransformerException, DOMException, IOException {
        Document dom = documentBuilder.parse(new ByteArrayInputStream(content));
        Element marcx = dom.getDocumentElement();
        Node child = marcx.getFirstChild();
        for (; ; ) {
            if (child == null ||
                    (child.getNodeType() == Node.ELEMENT_NODE &&
                            "datafield".equals(child.getLocalName()))) {
                int cmp = -1;
                if (child != null) {
                    String tag = ((Element) child).getAttribute("tag");
                    cmp = "004".compareTo(tag);
                }
                if (child == null || cmp < 0) {
                    Element n = dom.createElementNS("info:lc/xmlns/marcxchange-v1", "datafield");
                    n.setAttribute("tag", "004");
                    n.setAttribute("ind1", "0");
                    n.setAttribute("ind2", "0");
                    marcx.insertBefore(n, child);
                    child = n;
                }
                if (cmp <= 0) {
                    Node subChild = child.getFirstChild();
                    for (; ; ) {
                        // http://www.kat-format.dk/danMARC2/Danmarc2.7.htm
                        // r is 1st field
                        if (subChild == null || (subChild.getNodeType() == Node.ELEMENT_NODE &&
                                "subfield".equals(subChild.getLocalName()))) {
                            boolean isR = false;
                            if (subChild != null) {
                                String code = ((Element) subChild).getAttribute("code");
                                isR = "r".equals(code);
                            }
                            if (!isR) {
                                Element n = dom.createElementNS("info:lc/xmlns/marcxchange-v1", "subfield");
                                n.setAttribute("code", "r");
                                child.insertBefore(n, subChild);
                                subChild = n;
                            }
                            while (subChild.hasChildNodes()) {
                                subChild.removeChild(subChild.getFirstChild());
                            }
                            subChild.appendChild(dom.createTextNode("d"));
                            subChild = subChild.getNextSibling();
                            while (subChild != null) {
                                Node next = subChild.getNextSibling();
                                if (subChild.getNodeType() == Node.ELEMENT_NODE &&
                                        "subfield".equals(subChild.getLocalName()) &&
                                        "r".equals(((Element) subChild).getAttribute("code"))) {
                                    child.removeChild(subChild);
                                }
                                subChild = next;
                            }
                            break;

                        }
                        subChild = subChild.getNextSibling();
                    }
                    break;
                }
            }
            child = child.getNextSibling();
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        transformer.transform(
                new DOMSource(dom),
                new StreamResult(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
        return os.toByteArray();
    }

    /**
     * Create an xml document parser
     *
     * @return new document
     * @throws MarcXMergerException
     */
    private static DocumentBuilder newDocumentBuilder() throws MarcXMergerException {
        try {
            synchronized (DocumentBuilderFactory.class) {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                documentBuilderFactory.setIgnoringComments(true);
                documentBuilderFactory.setIgnoringElementContentWhitespace(true);
                return documentBuilderFactory.newDocumentBuilder();
            }
        } catch (ParserConfigurationException ex) {
            log.error("Cannot create parser part of xml merger", ex);
            throw new MarcXMergerException("Cannot init record merger", ex);
        }
    }

    /**
     * Create an xml transformer for writing a document
     *
     * @return new transformer
     * @throws MarcXMergerException
     */
    private static Transformer newTransformer() throws MarcXMergerException {
        try {
            synchronized (TransformerFactory.class) {
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.INDENT, "no");
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                return transformer;
            }
        } catch (TransformerFactoryConfigurationError | TransformerConfigurationException | IllegalArgumentException ex) {
            log.error("Cannot create writer part of xml merger", ex);
            throw new MarcXMergerException("Cannot init record merger", ex);
        }
    }

}
