package dk.dbc.rawrepo.maintain;

import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.maintain.transport.StandardResponse;
import dk.dbc.rawrepo.maintain.transport.StandardResponse.Result.Status;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
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

/**
 * @author DBC {@literal <dbc.dk>}
 */
class RemoveRecords extends RawRepoWorker {
    private static final Logger log = LoggerFactory.getLogger(RemoveRecords.class);

    private final DocumentBuilder documentBuilder;
    private final Transformer transformer;

    RemoveRecords(DataSource dataSource, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) throws MarcXMergerException {
        super(dataSource, vipCoreLibraryRulesConnector);
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

    Object removeRecords(Integer agencyId, List<String> bibliographicRecordIds, String provider, String trackingId) {
        log.debug("agencyId = " + agencyId +
                "; bibliographicRecordIds = " + bibliographicRecordIds +
                "; provider = " + provider +
                "; trackingId = " + trackingId);
        final ArrayList<StandardResponse.Result.Diag> diags = new ArrayList<>();
        int success = 0;
        int failed = 0;
        try {
            final Connection connection = getConnection();

            for (String bibliographicRecordId : bibliographicRecordIds) {
                log.info("remove: " + bibliographicRecordId + ":" + agencyId);
                connection.setAutoCommit(false);
                try {
                    removeRecord(agencyId, bibliographicRecordId, provider, trackingId);
                    connection.commit();
                    success++;
                } catch (VipCoreException | RawRepoException | DOMException | IOException | SAXException | TransformerException ex) {
                    failed++;
                    diags.add(new StandardResponse.Result.Diag("Record: " + bibliographicRecordId, ex.getMessage()));
                    final Throwable cause = ex.getCause();
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
            final StringBuilder message = new StringBuilder();
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

    void removeRecord(Integer agencyId, String bibliographicRecordId, String provider, String trackingId) throws RawRepoException, SAXException, TransformerException, DOMException, IOException, VipCoreException {
        final RawRepoDAO dao = getDao();

        if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
            throw new RawRepoException("Record doesn't exist");
        }

        final Record record = dao.fetchRecord(bibliographicRecordId, agencyId);

        if (record.isDeleted()) {
            throw new RawRepoException("Record already deleted");
        }
        if (!dao.getRelationsChildren(record.getId()).isEmpty()) {
            throw new RawRepoException("There are relations to this record (has children)");
        }
        if (!dao.getRelationsSiblingsToMe(record.getId()).isEmpty()) {
            throw new RawRepoException("There are relations to this record (has siblings)");
        }

        dao.changedRecord(provider, record.getId());

        final byte[] content = markMarcContentDeleted(record.getContent());
        record.setContent(content);
        record.setDeleted(true);
        if (trackingId != null) {
            record.setTrackingId(trackingId);
        }
        dao.deleteRelationsFrom(record.getId());
        dao.saveRecord(record);
    }

    private byte[] markMarcContentDeleted(byte[] content) throws SAXException, TransformerException, DOMException, IOException {
        final Document dom = documentBuilder.parse(new ByteArrayInputStream(content));
        final Element marcx = dom.getDocumentElement();
        Node child = marcx.getFirstChild();
        for (; ; ) {
            if (child == null ||
                    child.getNodeType() == Node.ELEMENT_NODE &&
                            "datafield".equals(child.getLocalName())) {
                int cmp = -1;
                if (child != null) {
                    final String tag = ((Element) child).getAttribute("tag");
                    cmp = "004".compareTo(tag);
                }
                if (child == null || cmp < 0) {
                    final Element n = dom.createElementNS("info:lc/xmlns/marcxchange-v1", "datafield");
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
                        if (subChild == null || subChild.getNodeType() == Node.ELEMENT_NODE &&
                                "subfield".equals(subChild.getLocalName())) {
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
                final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
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
                final TransformerFactory transformerFactory = TransformerFactory.newInstance();
                final Transformer transformer = transformerFactory.newTransformer();
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
