/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.marcxmerge;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author DBC {@literal <dbc.dk>}
 */
@RunWith(Parameterized.class)
public class MarcXMergerTest {

    String base;

    @Rule
    public MarcXResource resource = new MarcXResource();

    MarcXCompare marcXCompare;
    FieldRules fieldRulesIntermediate;
    FieldRules fieldRulesFinal;
    boolean isFinal;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    public MarcXMergerTest(String base, String immutable, String overwrite, String invalid, String valid_regex, String isFinal)
            throws ParserConfigurationException, TransformerConfigurationException {
        this.marcXCompare = new MarcXCompare();
        this.base = base;
        this.fieldRulesIntermediate = new FieldRules(immutable, overwrite, invalid, valid_regex);
        this.fieldRulesFinal = new FieldRules(immutable, overwrite, invalid, ".{3}");
        this.isFinal = Boolean.valueOf(isFinal);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection filenames() {
        return Arrays.asList(new String[][]{ // Constructor arguments
                {"append", "", "", "", ".*", "false"}, // Append
                {"immutable", "245", "", "", ".*", "false"}, // Don't overwrite procted field
                {"overwrite", "", "245", "", ".*", "false"}, // Overwrite of single fields
                {"overwrite_multi_to_single", "", "006;300", "", ".*", "false"},
                {"overwrite_multi", "", "245", "", ".*", "false"}, // Overwrite of repeated field
                {"overwrite_group", "", "245 239", "", ".*", "false"}, // Overwrite of repeated field
                {"remove", "", "", "245", ".*", "false"}, // Remove field
                {"invalid", "", "", "", "\\d{3}", "false"}, // Pattern validation
                {"isfinal", "", "", "", "\\d{3}", "true"} // Pattern validation
        });
    }

    @Test
    public void testMerge() throws SAXException, MarcXMergerException, IOException, UnsupportedEncodingException, TransformerException {

        System.out.println("base = " + base);
        byte[] common = resource.get(base + "/common");
        byte[] local = resource.get(base + "/local");
        byte[] result = resource.get(base + "/result");

        MarcXMerger marcxMerger = new MarcXMerger(fieldRulesIntermediate, "custom");
        byte[] merge = marcxMerger.merge(common, local, isFinal);
        marcXCompare.compare(result, merge);
    }

    @Test
    public void testCanMerge() throws MarcXMergerException {
        MarcXMerger marcxMerger = new MarcXMerger(fieldRulesIntermediate, "custom");

        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.MARCXCHANGE, MarcXChangeMimeType.ENRICHMENT), true);
        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.ARTICLE, MarcXChangeMimeType.ENRICHMENT), true);
        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.ENRICHMENT, MarcXChangeMimeType.ENRICHMENT), false);
        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.ENRICHMENT, MarcXChangeMimeType.ARTICLE), false);
        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.ENRICHMENT, MarcXChangeMimeType.MARCXCHANGE), false);
        assertEquals(marcxMerger.canMerge(MarcXChangeMimeType.AUTHORITY, MarcXChangeMimeType.ENRICHMENT), true);
    }

    @Test
    public void testMergedMimetype() throws MarcXMergerException {
        MarcXMerger marcxMerger = new MarcXMerger(fieldRulesIntermediate, "custom");

        assertEquals(marcxMerger.mergedMimetype(MarcXChangeMimeType.MARCXCHANGE, MarcXChangeMimeType.ENRICHMENT), MarcXChangeMimeType.MARCXCHANGE);
        assertEquals(marcxMerger.mergedMimetype(MarcXChangeMimeType.ARTICLE, MarcXChangeMimeType.ENRICHMENT), MarcXChangeMimeType.ARTICLE);
        assertEquals(marcxMerger.mergedMimetype(MarcXChangeMimeType.AUTHORITY, MarcXChangeMimeType.ENRICHMENT), MarcXChangeMimeType.AUTHORITY);
    }


    //  _   _      _                   _____                 _   _
    // | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|
    public static class MarcXResource extends ExternalResource {

        public byte[] get(String id) throws IOException {
            InputStream stream = getClass().getResourceAsStream("/" + id + ".xml");
            if (stream == null) {
                throw new IllegalStateException("Cannot open file: /" + id + ".xml");
            }
            int bytes = stream.available();
            byte[] data = new byte[bytes];
            if (stream.read(data) != bytes) {
                throw new IllegalStateException("Cannot read all content");
            }
            if (stream.available() != 0) {
                throw new IllegalStateException("Not all bytes marked as available");
            }
            return data;
        }
    }

    /**
     * Dom comparator
     */
    public static class MarcXCompare {

        private final DocumentBuilderFactory documentBuilderFactory;
        private final DocumentBuilder documentBuilder;
        private final TransformerFactory transformerFactory;
        private final Transformer transformer;

        public MarcXCompare() throws ParserConfigurationException, TransformerConfigurationException {
            synchronized (DocumentBuilderFactory.class) {
                documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                documentBuilderFactory.setIgnoringComments(true);
                documentBuilderFactory.setIgnoringElementContentWhitespace(true);
                documentBuilder = documentBuilderFactory.newDocumentBuilder();
            }
            synchronized (TransformerFactory.class) {
                transformerFactory = TransformerFactory.newInstance();
                transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.INDENT, "no");
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            }
        }

        public void compare(byte[] expected, byte[] actual) throws SAXException, IOException, UnsupportedEncodingException, TransformerException {
            Document expectedDom = documentBuilder.parse(new ByteArrayInputStream(expected));
            Document actualDom = documentBuilder.parse(new ByteArrayInputStream(actual));
            Element expectedRoot = expectedDom.getDocumentElement();
            Element actualRoot = actualDom.getDocumentElement();
            compare(expectedRoot, actualRoot, "");
        }

        private void compare(Node expected, Node actual, String location) throws UnsupportedEncodingException, TransformerException {
            short type = expected.getNodeType();
            assertEquals("Different nodetypes @" + location, type, actual.getNodeType());

            if (type == Node.TEXT_NODE) {
                assertEquals("Different value @" + location, expected.getNodeValue(), actual.getNodeValue());
            } else {
                assertEquals("Different node @" + location, expected.getLocalName(), actual.getLocalName());

                NamedNodeMap expectedAttributes = expected.getAttributes();
                NamedNodeMap actualAttributes = actual.getAttributes();
                ArrayList<String> expectedAttributeNames = attributesInSet(expectedAttributes);
                ArrayList<String> actualAttributeNames = attributesInSet(actualAttributes);
                assertEquals("Different attributes @" + location + "<" + expected.getLocalName() + ">", expectedAttributeNames, actualAttributeNames);
                for (String attr : actualAttributeNames) {
                    String expectedValue = expectedAttributes.getNamedItem(attr).getNodeValue();
                    String actualValue = actualAttributes.getNamedItem(attr).getNodeValue();
                    assertEquals("Different attributes @" + location + "<" + expected.getLocalName() + ">#" + attr,
                            expectedValue, actualValue);
                }

                location = location + nodeText(expected);

                Iterator<Node> expectedNodes = nodeIterator(expected);
                Iterator<Node> actualNodes = nodeIterator(actual);
                for (; ; ) {
                    Node expectedNode = nextNode(expectedNodes);
                    Node actualNode = nextNode(actualNodes);
                    if (expectedNode == null && actualNode == null) {
                        return;
                    }
                    if (expectedNode != null && actualNode == null) {
                        fail("Missing node @" + location + ": " + nodeText(expectedNode));
                    }
                    if (expectedNode == null && actualNode != null) {
                        fail("Extra node @" + location + ": " + nodeText(actualNode));
                    }
                    compare(expectedNode, actualNode, location);
                }

            }
        }

        private String nodeText(Node node) throws UnsupportedEncodingException, TransformerException {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            transformer.transform(new DOMSource(node.cloneNode(false)),
                    new StreamResult(new OutputStreamWriter(os, "UTF-8")));
            final String nodeText = os.toString("UTF-8");
            return nodeText;
        }

        private ArrayList<String> attributesInSet(NamedNodeMap attributes) {
            ArrayList<String> names = new ArrayList<>();
            for (int i = 0; i < attributes.getLength(); i++) {
                names.add(attributes.item(i).getNodeName());
            }
            Collections.sort(names);
            return names;
        }

        private static Iterator<Node> nodeIterator(Node node) {
            final NodeList nodes = node.getChildNodes();
            return new Iterator<Node>() {
                int i = 0;

                @Override
                public boolean hasNext() {
                    return i < nodes.getLength();
                }

                @Override
                public Node next() {
                    return nodes.item(i++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Readonly");
                }
            };
        }

        private static Node nextNode(Iterator<Node> it) {
            while (it.hasNext()) {
                Node node = it.next();
                switch (node.getNodeType()) {
                    case Node.COMMENT_NODE:
                        break;
                    case Node.TEXT_NODE:
                        if (!node.getNodeValue().trim().equals("")) {
                            return node;
                        }
                        break;
                    default:
                        return node;
                }
            }
            return null;
        }
    }
}
