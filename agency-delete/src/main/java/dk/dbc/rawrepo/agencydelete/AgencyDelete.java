/*
 * dbc-rawrepo-agency-delete
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-delete.
 *
 * dbc-rawrepo-agency-delete is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-delete is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-delete.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DBC {@literal <dbc.dk>}
 */
class AgencyDelete {

    private static final Logger log = LoggerFactory.getLogger(AgencyDelete.class);

    private final int agencyid;
    private final Connection connection;
    private final RawRepoDAO dao;
    private final DocumentBuilder documentBuilder;
    private final Transformer transformer;
    private final MarcXMerger marcXMerger;
    private final List<Integer> commonAgencies;

    public AgencyDelete(String db, int agencyid, String vipCoreUrl) throws MarcXMergerException, SQLException, RawRepoException {
        this.agencyid = agencyid;
        this.connection = getConnection(db);
        final RawRepoDAO.Builder builder = RawRepoDAO.builder(connection);
        if (vipCoreUrl != null) {
            VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector = VipCoreLibraryRulesConnectorFactory.create(vipCoreUrl);
            builder.relationHints(new RelationHintsVipCore(vipCoreLibraryRulesConnector));
            RelationHintsVipCore relationHints = new RelationHintsVipCore(vipCoreLibraryRulesConnector);
            if (relationHints.usesCommonAgency(agencyid)) {
                this.commonAgencies = relationHints.get(agencyid);
            } else {
                this.commonAgencies = Collections.emptyList();
            }
        } else {
            builder.relationHints(new RelationHintsVipCore(null) {
                @Override
                public boolean usesCommonAgency(int agencyId) {
                    return false;
                }
            });
            this.commonAgencies = Collections.emptyList();
        }

        this.dao = builder.build();
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
        this.marcXMerger = new MarcXMerger();
    }

    private AgencyDelete() throws MarcXMergerException {
        this.agencyid = 0;
        this.connection = null;
        this.dao = null;
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
        this.marcXMerger = null;
        this.commonAgencies = null;
    }

    static AgencyDelete unittestObject() throws MarcXMergerException {
        return new AgencyDelete();
    }

    public Set<String> getIds() throws SQLException {
        final Set<String> ids = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM records WHERE deleted = false AND agencyid = ?")) {
            stmt.setInt(1, agencyid);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getString(1));
                }
            }
        }
        final Map<String, Collection<String>> children = getChildrenRelationMap();
        final LinkedHashSet<String> nodes = new LinkedHashSet<>();
        for (String id : ids) {
            expandChildren(id, children, nodes);
        }
        return nodes;
    }

    private static void expandChildren(String bibliographicRecordId, Map<String, Collection<String>> children, LinkedHashSet<String> nodes) {
        if (nodes.contains(bibliographicRecordId)) {
            return;
        }
        final Collection<String> childIds = children.get(bibliographicRecordId);
        if (childIds != null) {
            for (String childId : childIds) {
                expandChildren(childId, children, nodes);
            }
        }
        nodes.add(bibliographicRecordId);
    }

    private Map<String, Collection<String>> getChildrenRelationMap() throws SQLException {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT refer_bibliographicrecordid, bibliographicrecordid FROM relations WHERE agencyid = refer_agencyid AND agencyid IN (");
        sb.append(agencyid);
        for (Integer commonAgency : commonAgencies) {
            sb.append(", ").append(commonAgency);
        }
        sb.append(")");
        log.debug("sb = {}", sb);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(sb.toString())) {
                final Map<String, Collection<String>> ret = new HashMap<>();
                while (resultSet.next()) {
                    Collection<String> col = ret.computeIfAbsent(resultSet.getString(1), k -> new HashSet<>());
                    col.add(resultSet.getString(2));
                }
                return ret;
            }
        }
    }

    /**
     * Create an xml document parser
     *
     * @return DocumentBuilder
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

    public Set<String> getSiblingRelations() throws SQLException {
        final Set<String> set = new HashSet<>();
        try (PreparedStatement stmt = connection.prepareStatement("SELECT bibliographicrecordid FROM relations WHERE refer_agencyid = ? AND agencyid <> refer_agencyid")) {
            stmt.setInt(1, agencyid);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    set.add(resultSet.getString(1));
                }
            }
        }
        return set;
    }

    void queueRecords(Set<String> ids, String provider) throws RawRepoException {
        for (String id : ids) {
            dao.enqueue(new RecordId(id, agencyid), provider, true, true);
        }
    }

    void deleteRecords(Set<String> ids) throws RawRepoException, MarcXMergerException, SAXException, IOException, TransformerException {
        log.debug("Setting content of records to deleted");
        for (String id : ids) {
            dao.deleteRelationsFrom(new RecordId(id, agencyid));
        }
        for (String id : ids) {
            Record record = dao.fetchMergedRecord(id, agencyid, marcXMerger, true);
            if (record.getId().getAgencyId() != agencyid) {
                log.debug("Creating record for: " + id);
                Record r = dao.fetchRecord(id, agencyid);
                r.setContent(record.getContent());
                r.setMimeType(record.getMimeType());
                record = r;
            }
            final byte[] content = markMarcContentDeleted(record.getContent());
            record.setContent(content);
            record.setDeleted(true);
            dao.saveRecord(record);
        }
    }

    byte[] markMarcContentDeleted(byte[] content) throws SAXException, TransformerException, DOMException, IOException {
        final Document dom = documentBuilder.parse(new ByteArrayInputStream(content));
        final Element marcx = dom.getDocumentElement();
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
                if (cmp < 0) {
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

    public void begin() throws SQLException {
        connection.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        connection.commit();
    }

    private static final Pattern urlPattern = Pattern.compile("^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$");
    private static final String JDBC_DEFAULT = "jdbc:postgresql://";
    private static final int URL_PATTERN_PREFIX = 1;
    private static final int URL_PATTERN_USER = 2;
    private static final int URL_PATTERN_PASSWORD = 3;
    private static final int URL_PATTERN_HOST_PORT_DB = 4;

    private static Connection getConnection(String url) throws SQLException {
        final Matcher matcher = urlPattern.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(url + " Is not a valid jdbc uri");
        }
        final Properties properties = new Properties();
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
        final Connection connection = DriverManager.getConnection(jdbc + matcher.group(URL_PATTERN_HOST_PORT_DB), properties);
        log.debug("Connected");
        return connection;
    }

}
