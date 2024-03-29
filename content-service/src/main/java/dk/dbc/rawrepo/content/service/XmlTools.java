package dk.dbc.rawrepo.content.service;

import dk.dbc.marcxmerge.MarcXMergerException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.regex.Pattern;
import javax.ejb.EJBException;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
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
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class XmlTools {

    private static final Logger log = LoggerFactory.getLogger(XmlTools.class);

    private static final String MARCX_NS = "info:lc/xmlns/marcxchange-v1";
    private static final String MARCX_PREFIX = "marcx";

    private static final Pattern PRIVATE_TAG = Pattern.compile(".*[^0-9].*");

    private DocumentBuilder documentBuilder;
    private Transformer transformer;
    private XPathFactory xpathFactory;

    public XmlTools() {
        log.debug("init");
        try {
            this.documentBuilder = newDocumentBuilder();
            this.transformer = newTransformer();
            this.xpathFactory = newXpathFactory();
        } catch (MarcXMergerException ex) {
            throw new EJBException("Error initializing XmlTools", ex);
        }
    }

    public byte[] filterPrivateOut(byte[] src) {
        try {
            Document dom = documentBuilder.parse(new ByteArrayInputStream(src));
            XPath xPath = xpathFactory.newXPath();
            xPath.setNamespaceContext(MARCX_NAMESPACE_CONTEXT);
            XPathExpression expression = xPath.compile("//marcx:datafield[@tag]");
            NodeList nodes = (NodeList) expression.evaluate(dom, XPathConstants.NODESET);
            for (int i = 0 ; i < nodes.getLength() ; i++) {
                Node item = nodes.item(i);
                String fieldname = item.getAttributes().getNamedItem("tag").getNodeValue();
                if (PRIVATE_TAG.matcher(fieldname).matches()) {
                    log.debug("removing datafield: {}", fieldname);
                    Node next = item.getNextSibling();
                    if (next != null && next.getNodeType() == Node.TEXT_NODE) {
                        item.getParentNode().removeChild(next);
                    }
                    item.getParentNode().removeChild(item);
                }
            }
            return documentToBytes(dom);
        } catch (XPathExpressionException | SAXException | IOException | TransformerException ex) {
            log.error("Exception " + ex.getClass().getName(), ex);
            return src;
        }
    }

    public MarcXCollection buildCollection() {
        return new MarcXCollection();
    }

    public class MarcXCollection {

        private final Document dom;
        private final Element collection;

        private MarcXCollection() {
            dom = documentBuilder.newDocument();
            dom.setXmlStandalone(true);
            collection = dom.createElementNS(MARCX_NS, "collection");
            collection.setPrefix(MARCX_PREFIX);
            dom.appendChild(collection);
        }

        public MarcXCollection add(byte[] doc) {
            try {
                Document docDom = documentBuilder.parse(new ByteArrayInputStream(doc));
                Element element = docDom.getDocumentElement();
                if (element.getNamespaceURI().equals(MARCX_NS)
                    && element.getLocalName().equals("record")) {
                    collection.appendChild(dom.adoptNode(element));
                } else {
                    log.warn("Cannot add element: " + element.getNodeName());
                }
            } catch (SAXException | IOException ex) {
                log.error("Exception " + ex.getClass().getName(), ex);
            }
            return this;

        }

        public byte[] build() {
            byte[] bytes = null;
            try {
                bytes = documentToBytes(dom);
            } catch (UnsupportedEncodingException | TransformerException ex) {
                log.error("Exception " + ex.getClass().getName(), ex);
            }
            return bytes;
        }
    }

    /**
     * Namespace resolver for marcx
     */
    private static final NamespaceContext MARCX_NAMESPACE_CONTEXT = new NamespaceContext() {

        @Override
        public String getNamespaceURI(String prefix) {
            if (MARCX_PREFIX.equals(prefix)) {
                return MARCX_NS;
            }
            if ("xml".equals(prefix)) {
                return XMLConstants.XML_NS_URI;
            }
            return XMLConstants.NULL_NS_URI;
        }

        @Override
        public String getPrefix(String namespaceURI) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator getPrefixes(String namespaceURI) {
            throw new UnsupportedOperationException();
        }
    };

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
     * Create a xml transformer for writing a document
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

    private static XPathFactory newXpathFactory() {
        synchronized (XPathFactory.class) {
            return XPathFactory.newInstance();
        }
    }

    /**
     * Convert a xml document into a bytearray
     *
     * @param dom
     * @return
     * @throws UnsupportedEncodingException
     * @throws TransformerException
     */
    private byte[] documentToBytes(Document dom) throws UnsupportedEncodingException, TransformerException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        transformer.transform(new DOMSource(dom),
                              new StreamResult(new OutputStreamWriter(os, "UTF-8")));
        return os.toByteArray();
    }

}
