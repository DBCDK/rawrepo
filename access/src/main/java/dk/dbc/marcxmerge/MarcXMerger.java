/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.marcxmerge;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ListIterator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class MarcXMerger {

    private static final Logger log = LoggerFactory.getLogger(MarcXMerger.class);

    public enum DataFieldAction {

        Immutable,
        Append,
        Overwrite,
        Remove
    };

    private static final String MARCX_NS = "info:lc/xmlns/marcxchange-v1";
    private static final String ATTRIBUTE_TAG = "tag";
    private static final String ELEMENT_RECORD = "record";
    private static final String ELEMENT_DATAFIELD = "datafield";
    private static final String UTF8 = "UTF-8";

    private final DocumentBuilder documentBuilder;
    private final Transformer transformer;
    private final FieldRules fieldRulesIntermediate;

    /**
     * Default constructor, sets up FieldRules according to std rules
     *
     * @throws MarcXMergerException
     */
    public MarcXMerger() throws MarcXMergerException {
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
        this.fieldRulesIntermediate = new FieldRules();

    }

    /**
     * Constructor for custom FieldRules
     *
     * @param fieldRulesIntermediate ruleset for merging records
     * @throws MarcXMergerException
     */
    public MarcXMerger(FieldRules fieldRulesIntermediate) throws MarcXMergerException {
        this.documentBuilder = newDocumentBuilder();
        this.transformer = newTransformer();
        this.fieldRulesIntermediate = fieldRulesIntermediate;
    }

    public boolean canMerge(String originalMimeType, String enrichmentMimeType) {
        switch (originalMimeType) {
            case MarcXChangeMimeType.MARCXCHANGE:
                switch (enrichmentMimeType) {
                    case MarcXChangeMimeType.ENRICHMENT:
                        return true;
                }
        }
        return false;
    }

    public String mergedMimetype(String originalMimeType, String enrichmentMimeType) {
        switch (originalMimeType) {
            case MarcXChangeMimeType.MARCXCHANGE:
                switch (enrichmentMimeType) {
                    case MarcXChangeMimeType.ENRICHMENT:
                        return MarcXChangeMimeType.MARCXCHANGE;
                }
        }
        throw new IllegalStateException("Cannot figure out mimetype of: " + originalMimeType + "&" + enrichmentMimeType);
    }

    /**
     * Create an xml document parser
     *
     * @return
     * @throws ParserConfigurationException
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
     * @throws TransformerConfigurationException
     * @throws TransformerFactoryConfigurationError
     * @throws IllegalArgumentException
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

    /**
     * Merge two marcxchange records according to the rules defined in the
     * constructor
     *
     * @param common           the base of the result
     * @param local            the additional data
     * @param includeAllFields
     * @return a merged record
     * @throws MarcXMergerException
     */
    public byte[] merge(byte[] common, byte[] local, boolean includeAllFields) throws MarcXMergerException {
        try {
            Document commonDom = documentBuilder.parse(new ByteArrayInputStream(common));
            Document localDom = documentBuilder.parse(new ByteArrayInputStream(local));
            Element commonRootElement = commonDom.getDocumentElement();
            Element localRootElement = localDom.getDocumentElement();

            if (!commonRootElement.getLocalName().equals(ELEMENT_RECORD)
                || !commonRootElement.getNamespaceURI().equals(MARCX_NS)) {
                throw new MarcXMergerException("Outermost tag in common record: "
                                               + "{" + commonRootElement.getNamespaceURI() + "}" + commonRootElement.getLocalName()
                                               + " Expected {" + MARCX_NS + "}" + ELEMENT_RECORD);
            }

            if (!localRootElement.getLocalName().equals(ELEMENT_RECORD)
                || !localRootElement.getNamespaceURI().equals(MARCX_NS)) {
                throw new MarcXMergerException("Outermost tag in local record: "
                                               + "{" + localRootElement.getNamespaceURI() + "}" + localRootElement.getLocalName()
                                               + " Expected {" + MARCX_NS + "}" + ELEMENT_RECORD);
            }

            Document targetDom = commonDom; // reuse common dom's leader
            Element targetRootElement = commonRootElement;

            FieldRules.RuleSet ruleSet = fieldRulesIntermediate.newRuleSet();

            removeEmptyText(commonDom.getDocumentElement()); // cleanup nodes
            removeEmptyText(localDom.getDocumentElement()); // cleanup nodes

            ArrayList<Node> localFields = getDatafields(localDom.getDocumentElement());
            ArrayList<Node> commonFields = getDatafields(commonDom.getDocumentElement());

            removeRegisterAndImportLocalFields(localFields, ruleSet, targetDom, includeAllFields); // sets up which common fields, that should be removed
            removeAndImportCommonFields(commonFields, ruleSet, targetDom);

            mergeCommonAndLocalIntoTarget(localFields, commonFields, targetRootElement);

            return documentToBytes(targetDom);
        } catch (SAXException | IOException | TransformerException ex) {
            log.error("Cannot merge records", ex);
            throw new MarcXMergerException("Record merge error", ex);
        }
    }

    /**
     * Traverse an xml document element removing empty text/cdata nodes
     *
     * @param element
     */
    private static void removeEmptyText(Element element) {
        Node child = element.getFirstChild();
        while (child != null) {
            Node nextSibling = child.getNextSibling();
            if (child.getNodeType() == Node.TEXT_NODE) {
                if (child.getNodeValue().trim().equals("")) {
                    element.removeChild(child);
                }
            } else if (child.getNodeType() == Node.CDATA_SECTION_NODE) {
                if (child.getNodeValue().trim().equals("")) {
                    element.removeChild(child);
                }
            } else if (child.getNodeType() == Node.ELEMENT_NODE) {
                removeEmptyText((Element) child);
            }
            child = nextSibling;
        }
    }

    /**
     * removes unwanted nodes from localFields
     *
     * removes nodes from localDom
     *
     * imports nodes into targetDom
     *
     * registers tags in ruleSet, selecting which to remove from commonFields
     *
     * @param localFields
     * @param ruleSet
     * @param targetDom
     * @throws DOMException
     */
    private static void removeRegisterAndImportLocalFields(ArrayList<Node> localFields, FieldRules.RuleSet ruleSet, Document targetDom, boolean includeAllFields) throws DOMException {
        for (ListIterator<Node> it = localFields.listIterator() ; it.hasNext() ;) {
            Element element = (Element) it.next();
            String tag = element.getAttribute(ATTRIBUTE_TAG);
            element.getParentNode().removeChild(element);
            if (ruleSet.immutableField(tag) || ruleSet.invalidField(tag, includeAllFields)) {
                it.remove();
            } else {
                it.set(targetDom.importNode(element, true));
                ruleSet.registerLocalField(tag);
            }
        }
    }

    /**
     * removes nodes from commonFields according to ruleSet
     *
     * imports nodes into targetDom
     *
     * removes nodes from commonDom
     *
     * @param commonFields
     * @param ruleSet
     * @param targetDom
     * @throws DOMException
     */
    private static void removeAndImportCommonFields(ArrayList<Node> commonFields, FieldRules.RuleSet ruleSet, Document targetDom) throws DOMException {
        for (ListIterator<Node> it = commonFields.listIterator() ; it.hasNext() ;) {
            Element element = (Element) it.next();
            String tag = element.getAttribute(ATTRIBUTE_TAG);
            element.getParentNode().removeChild(element);
            if (ruleSet.invalidField(tag, false) || ruleSet.removeField(tag)) {
                it.remove();
            } else {
                it.set(targetDom.importNode(element, true));
            }
        }
    }

    /**
     *
     * @param localFields
     * @param commonFields
     * @param targetElement
     * @throws DOMException
     */
    private static void mergeCommonAndLocalIntoTarget(ArrayList<Node> localFields, ArrayList<Node> commonFields, Element targetElement) throws DOMException {
        ListIterator<Node> localIterator = localFields.listIterator();
        ListIterator<Node> commonIterator = commonFields.listIterator();

        while (commonIterator.hasNext() && localIterator.hasNext()) {
            Element commonElement = (Element) commonIterator.next();
            String commonTag = commonElement.getAttribute(ATTRIBUTE_TAG);
            Element localElement = (Element) localIterator.next();
            String localTag = localElement.getAttribute(ATTRIBUTE_TAG);
            if (commonTag.compareTo(localTag) <= 0) {
                localIterator.previous();
                targetElement.appendChild(commonElement);
            } else {
                commonIterator.previous();
                targetElement.appendChild(localElement);
            }
        }
        while (commonIterator.hasNext()) {
            Element element = (Element) commonIterator.next();
            targetElement.appendChild(element);
        }
        while (localIterator.hasNext()) {
            Element element = (Element) localIterator.next();
            targetElement.appendChild(element);
        }
    }

    /**
     * Implementation to sort datafields by tag
     */
    private static Comparator<Node> NODE_TAG_COMPARE = new Comparator<Node>() {
        @Override
        public int compare(Node o1, Node o2) {
            if (o1.getNodeType() == Node.ELEMENT_NODE
                && o2.getNodeType() == Node.ELEMENT_NODE
                && ( (Element) o1 ).hasAttribute(ATTRIBUTE_TAG)
                && ( (Element) o2 ).hasAttribute(ATTRIBUTE_TAG)) {
                String o1Tag = ( (Element) o1 ).getAttribute(ATTRIBUTE_TAG);
                String o2Tag = ( (Element) o2 ).getAttribute(ATTRIBUTE_TAG);
                return o1Tag.compareTo(o2Tag);
            } else {
                return o2.hashCode() - o1.hashCode();
            }
        }
    };

    /**
     * extract datafields from element, return them sorted by tag
     *
     * @param element
     * @return
     */
    private static ArrayList<Node> getDatafields(Element element) {
        NodeList nodeList = element.getElementsByTagNameNS(MARCX_NS, ELEMENT_DATAFIELD);
        ArrayList<Node> ret = new ArrayList<>(nodeList.getLength());
        for (int i = 0 ; i < nodeList.getLength() ; i++) {
            ret.add(nodeList.item(i));
        }
        Collections.sort(ret, NODE_TAG_COMPARE);
        return ret;
    }

    /**
     * Convert an xml document into a bytearray
     *
     * @param dom
     * @return
     * @throws UnsupportedEncodingException
     * @throws TransformerException
     */
    private byte[] documentToBytes(Document dom) throws UnsupportedEncodingException, TransformerException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        transformer.transform(new DOMSource(dom),
                              new StreamResult(new OutputStreamWriter(os, UTF8)));
        return os.toByteArray();
    }
}
