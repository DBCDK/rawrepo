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
package dk.dbc.marcx;

import dk.dbc.rawrepo.RecordId;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class MarcXParser extends DefaultHandler {

    public interface FieldProcessor {

        void field(String datafield, String subfield, String value);
    }

    public static class ParentField implements FieldProcessor {

        String parent;

        public ParentField() {
            this.parent = null;
        }

        public String getParent() {
            return parent;
        }

        public boolean hasParent() {
            return parent != null;
        }

        @Override
        public void field(String datafield, String subfield, String value) {
            if (datafield.equals("014") && subfield.equals("a")) {
                parent = value.trim();
            }
        }

    }

    public static class AuthorityFields implements FieldProcessor {
        private static final List<String> FIELDS_MAY_HAVE_AUT = Arrays.asList("100", "600", "700");
        List<RecordId> authorityLinks;

        public AuthorityFields() {
            this.authorityLinks = new ArrayList<>();
        }

        public List<RecordId> getAuthorityLinks() {
            return authorityLinks;
        }

        public boolean hasAut() {
            return authorityLinks.size() > 0;
        }

        @Override
        public void field(String datafield, String subfield, String value) {
            if (FIELDS_MAY_HAVE_AUT.contains(datafield) && "6".equals(subfield)) {
                authorityLinks.add(new RecordId(value, 870979));
            }
        }

    }


    private static final SAXParserFactory parserFactory = makeParserFactory();

    FieldProcessor processor;
    private String datafield;
    private String subfield;

    public MarcXParser(FieldProcessor processor) {
        this.processor = processor;
        this.datafield = null;
        this.subfield = null;
    }

    public static void parse(InputStream is, FieldProcessor processor) throws ParserConfigurationException, SAXException, IOException {
        MarcXParser marcXParser = new MarcXParser(processor);
        SAXParser parser = parserFactory.newSAXParser();
        parser.parse(is, marcXParser);
    }

    public static String getParent(InputStream is) throws ParserConfigurationException, SAXException, IOException {
        ParentField processor = new ParentField();
        parse(is, processor);
        return processor.getParent();
    }

    public static List<RecordId> getAuthorityLinks(InputStream is) throws ParserConfigurationException, SAXException, IOException {
        AuthorityFields processor = new AuthorityFields();
        parse(is, processor);
        return processor.getAuthorityLinks();
    }


    private static SAXParserFactory makeParserFactory() {
        synchronized (SAXParserFactory.class) {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setValidating(true);
            return factory;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (datafield != null && subfield != null) {
            String value = new String(ch, start, length);
            processor.field(datafield, subfield, value);
        }

    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        switch (localName) {
            case "datafield":
                datafield = attributes.getValue("tag");
                break;
            case "subfield":
                subfield = attributes.getValue("code");
                break;
            default:
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        switch (localName) {
            case "datafield":
                datafield = null;
                break;
            case "subfield":
                subfield = null;
                break;
            default:
                break;
        }
    }

}
