package dk.dbc.marcx.sax;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class MarcxParser extends DefaultHandler {

    String dataField;
    String subField;

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        String value = new String(ch, start, length);
        if (dataField != null && subField != null) {
            content(dataField, subField, value);
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.endsWith(":datafield")) {
            dataField = attributes.getValue("tag");
        }
        if (qName.endsWith(":subfield")) {
            subField = attributes.getValue("code");
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.endsWith(":datafield")) {
            dataField = null;
        }
        if (qName.endsWith(":subfield")) {
            subField = null;
        }
    }

    public void content(String dataField, String subField, String value) {
        System.out.println("dataField = " + dataField + ", subField = " + subField + ", value = " + value);
    }

}
