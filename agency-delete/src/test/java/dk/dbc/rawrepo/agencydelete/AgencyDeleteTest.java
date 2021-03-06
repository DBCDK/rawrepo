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

import dk.dbc.commons.testutils.postgres.connection.PostgresITConnection;
import dk.dbc.xmldiff.XmlDiff;
import dk.dbc.xmldiff.XmlDiffTextWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.junit.jupiter.api.Test;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.fail;


/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
class AgencyDeleteTest {

    private String jdbcUrl;
    private Connection connection;
    private PostgresITConnection postgresITConnection;

    public AgencyDeleteTest() {
    }


    @Test
    void MarkMarcContentDeleted() throws Exception {
        assertEqual(getResource("tmpl_has_004r_expected.xml"), getResource("tmpl_has_004r.xml"));
        assertEqual(getResource("tmpl_has_004_expected.xml"), getResource("tmpl_has_004.xml"));
        assertEqual(getResource("tmpl_has_no_004_expected.xml"), getResource("tmpl_has_no_004.xml"));
        assertEqual(getResource("tmpl_has_004r_misplaced_expected.xml"), getResource("tmpl_has_004r_misplaced.xml"));
    }

    private static  void assertEqual(byte[] expected, byte[] actual) throws DOMException, XPathExpressionException, Exception, TransformerException, IOException, SAXException {
        AgencyDelete agencyDelete = AgencyDelete.unittestObject();
        actual = agencyDelete.markMarcContentDeleted(actual);
        XmlDiff xmlDiff = XmlDiff.builder().indent(2).normalize(true).strip(true).trim(true).build();
        XmlDiffTextWriter xmlDiffTextWriter = new XmlDiffTextWriter("{-", "-}", "{+", "+}", "", "");
        boolean identical = xmlDiff.compare(new ByteArrayInputStream(expected), new ByteArrayInputStream(actual), xmlDiffTextWriter);
        if(!identical) {
            fail("Xml Mismatch: " + xmlDiffTextWriter.toString());
        }
    }

    private static byte[] getResource(String res) {
        try {
            try (InputStream is = AgencyDeleteTest.class.getClassLoader().getResourceAsStream(res)) {
                assert is != null;
                int available = is.available();
                byte[] bytes = new byte[available];
                is.read(bytes);
                return bytes;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
