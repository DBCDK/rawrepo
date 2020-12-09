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


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class MarcXMergerTest {
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

    @ParameterizedTest
    @MethodSource("filenames")
    public void testMerge(String base, String immutableString, String overwriteString, String invalidString, String valid_regex, String isFinalString)
            throws SAXException, MarcXMergerException, IOException, TransformerException, ParserConfigurationException {
        final MarcXCompare marcXCompare = new MarcXCompare();
        final FieldRules fieldRulesIntermediate = new FieldRules(immutableString, overwriteString, invalidString, valid_regex);
        final boolean isFinal = Boolean.parseBoolean(isFinalString);

        final MarcXResource resource = new MarcXResource();
        final byte[] common = resource.get(base + "/common");
        final byte[] local = resource.get(base + "/local");
        final byte[] result = resource.get(base + "/result");

        final MarcXMerger marcxMerger = new MarcXMerger(fieldRulesIntermediate, "custom");
        final byte[] merge = marcxMerger.merge(common, local, isFinal);
        marcXCompare.compare(result, merge);
    }


    //  _   _      _                   _____                 _   _
    // | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|
    public static class MarcXResource {

        public byte[] get(String id) throws IOException {
            final InputStream stream = getClass().getResourceAsStream("/" + id + ".xml");
            if (stream == null) {
                throw new IllegalStateException("Cannot open file: /" + id + ".xml");
            }
            final int bytes = stream.available();
            final byte[] data = new byte[bytes];
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
        private final DocumentBuilder documentBuilder;
        private final Transformer transformer;

        public MarcXCompare() throws ParserConfigurationException, TransformerConfigurationException {
            synchronized (DocumentBuilderFactory.class) {
                final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setNamespaceAware(true);
                documentBuilderFactory.setIgnoringComments(true);
                documentBuilderFactory.setIgnoringElementContentWhitespace(true);
                documentBuilder = documentBuilderFactory.newDocumentBuilder();
            }
            synchronized (TransformerFactory.class) {
                final TransformerFactory transformerFactory = TransformerFactory.newInstance();
                transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.INDENT, "no");
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            }
        }

        public void compare(byte[] expected, byte[] actual) throws SAXException, IOException, TransformerException {
            final Document expectedDom = documentBuilder.parse(new ByteArrayInputStream(expected));
            final Document actualDom = documentBuilder.parse(new ByteArrayInputStream(actual));
            final Element expectedRoot = expectedDom.getDocumentElement();
            final Element actualRoot = actualDom.getDocumentElement();
            compare(expectedRoot, actualRoot, "");
        }

        private void compare(Node expected, Node actual, String location) throws UnsupportedEncodingException, TransformerException {
            final short type = expected.getNodeType();
            assertThat("Different nodetypes @" + location, actual.getNodeType(), is(type));

            if (type == Node.TEXT_NODE) {
                assertThat("Different value @" + location, actual.getNodeValue(), is(expected.getNodeValue()));
            } else {
                assertThat("Different node @" + location, actual.getLocalName(), is(expected.getLocalName()));

                final NamedNodeMap expectedAttributes = expected.getAttributes();
                final NamedNodeMap actualAttributes = actual.getAttributes();
                final ArrayList<String> expectedAttributeNames = attributesInSet(expectedAttributes);
                final ArrayList<String> actualAttributeNames = attributesInSet(actualAttributes);
                assertThat("Different attributes @" + location + "<" + expected.getLocalName() + ">", actualAttributeNames, is(expectedAttributeNames));
                for (String attr : actualAttributeNames) {
                    final String expectedValue = expectedAttributes.getNamedItem(attr).getNodeValue();
                    final String actualValue = actualAttributes.getNamedItem(attr).getNodeValue();
                    assertThat("Different attributes @" + location + "<" + expected.getLocalName() + ">#" + attr,
                            actualValue, is(expectedValue));
                }

                location = location + nodeText(expected);

                final Iterator<Node> expectedNodes = nodeIterator(expected);
                final Iterator<Node> actualNodes = nodeIterator(actual);
                for (; ; ) {
                    final Node expectedNode = nextNode(expectedNodes);
                    final Node actualNode = nextNode(actualNodes);
                    if (expectedNode == null && actualNode == null) {
                        return;
                    }
                    if (expectedNode != null && actualNode == null) {
                        fail("Missing node @" + location + ": " + nodeText(expectedNode));
                    }
                    if (expectedNode == null) {
                        fail("Extra node @" + location + ": " + nodeText(actualNode));
                    }
                    compare(expectedNode, actualNode, location);
                }

            }
        }

        private String nodeText(Node node) throws UnsupportedEncodingException, TransformerException {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            transformer.transform(new DOMSource(node.cloneNode(false)),
                    new StreamResult(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
            return os.toString("UTF-8");
        }

        private ArrayList<String> attributesInSet(NamedNodeMap attributes) {
            final ArrayList<String> names = new ArrayList<>();
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
                final Node node = it.next();
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
