/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-load
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencyload;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class MarcXBlock {

    private static final Logger log = LoggerFactory.getLogger(MarcXBlock.class);

    private StringBuilder sb;
    private String opening;

    private final HashMap<String, String> namespaces;
    private final HashMap<String, String> namedNamespaces;
    private int nsNo = 0;
    private String pos;

    private final MarcXProcessor processor;

    /**
     *
     * @param processor
     */
    public MarcXBlock(MarcXProcessor processor) {
        this.processor = processor;
        this.opening = null;
        this.pos = "";
        this.sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"utf8\"?>\n");
        this.namespaces = new HashMap<>();
        this.namedNamespaces = new HashMap<>();
    }

    public void addPrefix(String prefix, String namespace) {
        this.namedNamespaces.put(namespace, prefix);
    }

    private String getPrefix(String ns) {
        if (ns == null || ns.isEmpty()) {
            return "";
        }
        String prefix = namespaces.get(ns);
        if (prefix == null) {
            prefix = namedNamespaces.get(ns);
            if (prefix == null) {
                prefix = "ns" + ( ++nsNo );
            }
            namespaces.put(ns, prefix);
        }
        return prefix + ":";
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        sb.append("<").append(getPrefix(uri)).append(localName);
        for (int i = 0 ; i < attributes.getLength() ; i++) {
            sb.append(" ")
                    .append(getPrefix(attributes.getURI(i)))
                    .append(attributes.getLocalName(i))
                    .append("=\"");
            writeEscaped(attributes.getValue(i), false);
            sb.append("\"");
        }
        if (opening == null) {
            opening = sb.toString();
            sb = new StringBuilder();
        }
        sb.append(">");
        switch (localName) {
            case "datafield":
                pos = attributes.getValue("tag");
                break;
            case "subfield":
                pos += attributes.getValue("code");
                break;
            default:
                break;
        }
    }


    public void characters(String data) {
        if (pos.length() == 4) {
            processor.marcxContent(pos, data);
        }
        writeEscaped(data, true);
    }

    private void writeEscaped(String data, boolean textEscape) {
        for (char c : data.toCharArray()) {
            if (textEscape || (c >= 0x20 && c <= 0x7F)) {
                if (c == '&') {
                    sb.append("&amp;");
                } else if (c == '<') {
                    sb.append("&lt;");
                } else if (c == '>') {
                    sb.append("&gt;");
                } else if (c == '"') {
                    sb.append("&quot;");
                } else if (c == '\'') {
                    sb.append("&apos;");
                } else {
                    sb.append(c);
                }
            } else {
                sb.append(String.format("&#x%04x;", 0xffff & (int) c));
            }
        }
    }

    public void endElement(String uri, String localName, String qName) {
        switch (localName) {
            case "datafield":
                pos = "";
                break;
            case "subfield":
                pos = pos.substring(0, 3);
                break;
            default:
                break;
        }
        sb.append("</").append(getPrefix(uri)).append(localName).append(">");
    }

    public void done() {
        String tail = sb.toString();
        sb = new StringBuilder(opening);

        for (Map.Entry<String, String> entry : namespaces.entrySet()) {
            sb.append(" xmlns:")
                    .append(entry.getValue())
                    .append("=\"");
            writeEscaped(entry.getKey(), false);
            sb.append("\"");
        }
        sb.append(tail);
        processor.marcxXml(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

}
