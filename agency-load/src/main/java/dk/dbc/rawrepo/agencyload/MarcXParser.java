/*
 * dbc-rawrepo-agency-load
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-load.
 *
 * dbc-rawrepo-agency-load is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-load is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-load.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencyload;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class MarcXParser extends DefaultHandler {

    private static final SAXParserFactory parserFactory = makeParserFactory();

    private int depth;
    private MarcXBlock block;
    private final MarcXProcessor processor;

    public MarcXParser(MarcXProcessor processor) {
        this.depth = -1;
        this.block = null;
        this.processor = processor;
    }

    public static void parse(InputStream is, MarcXProcessor processor) throws ParserConfigurationException, SAXException, IOException {
        MarcXParser marcXParser = new MarcXParser(processor);
        SAXParser parser = parserFactory.newSAXParser();
        parser.parse(is, marcXParser);
    }

    private static SAXParserFactory makeParserFactory() {
        synchronized (SAXParserFactory.class) {
            SAXParserFactory parserFactory = SAXParserFactory.newInstance();
            parserFactory.setNamespaceAware(true);
            parserFactory.setValidating(true);
            return parserFactory;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length); //To change body of generated methods, choose Tools | Templates.
        if (depth > 0) {
            this.block.characters(new String(ch, start, length));
        }

    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes); //To change body of generated methods, choose Tools | Templates.
        if (depth++ == 0) {
            block = processor.makeMarcXBlock();
        }
        if (depth > 0) {
            block.startElement(uri, localName, qName, attributes);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName); //To change body of generated methods, choose Tools | Templates.
        if (depth > 0) {
            block.endElement(uri, localName, qName);
        }
        if (--depth == 0) {
            block.done();
            block = null;
        }
    }

    @Override
    public void startDocument() throws SAXException {
        super.startDocument(); //To change body of generated methods, choose Tools | Templates.
        depth = -1;
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument(); //To change body of generated methods, choose Tools | Templates.
        if (depth != -1) {
            throw new SAXException("This is not closed properly");
        }
    }

}
