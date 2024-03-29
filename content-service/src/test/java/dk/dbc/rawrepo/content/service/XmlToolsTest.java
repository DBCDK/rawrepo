package dk.dbc.rawrepo.content.service;

import dk.dbc.xmldiff.XmlDiff;
import dk.dbc.xmldiff.XmlDiffTextWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * @author DBC {@literal <dbc.dk>}
 */
class XmlToolsTest {

    /**
     * Test of filterPrivateOut method, of class XmlTools.
     */
    @Test
    void testFilterPrivateOut() throws Exception {
        byte[] marcxWith = file("marcx-with-private.xml");
        byte[] marcxWithout = file("marcx-without-private.xml");

        XmlTools xmlTools = new FakeCDI().build(XmlTools.class);
        byte[] marcxFiltered = xmlTools.filterPrivateOut(marcxWith);

        XmlDiffTextWriter writer = new XmlDiffTextWriter("«A:", "»", "«E:", "»", "«NS:", "»");
        boolean equal = XmlDiff.builder().indent(2).normalize(true).strip(true).trim(true).build()
                .compare(new ByteArrayInputStream(marcxFiltered), new ByteArrayInputStream(marcxWithout), writer);
        if (!equal) {
            System.out.println("writer = " + writer.toString());
        }
        assertTrue(equal);
    }

    @Test
    void testCombine() throws Exception {

        XmlTools xmlTools = new FakeCDI().build(XmlTools.class);
        byte[] combinedActual = xmlTools.buildCollection().add(file("marcx-with-private_1.xml")).add(file("marcx-with-private_2.xml")).build();
        byte[] combinedExpected = file("marcx-with-private_combined.xml");

        XmlDiffTextWriter writer = new XmlDiffTextWriter("«A:", "»", "«E:", "»", "«NS:", "»");
        boolean equal = XmlDiff.builder().indent(2).normalize(true).strip(true).trim(true).build()
                .compare(new ByteArrayInputStream(combinedActual), new ByteArrayInputStream(combinedExpected), writer);
        if (!equal) {
            System.out.println("writer = " + writer.toString());
        }
        assertTrue(equal);

    }

    //  _   _      _                   _____                 _   _
    // | | | | ___| |_ __   ___ _ __  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
    // | |_| |/ _ \ | '_ \ / _ \ '__| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
    // |  _  |  __/ | |_) |  __/ |    |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
    // |_| |_|\___|_| .__/ \___|_|    |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
    //              |_|
    public byte[] file(String file) throws IOException {
        InputStream stream = getClass().getResourceAsStream("/" + file);
        if (stream == null) {
            throw new IllegalStateException("Cannot open file: /" + file);
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
