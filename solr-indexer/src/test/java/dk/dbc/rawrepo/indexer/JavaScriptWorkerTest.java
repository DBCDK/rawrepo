/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.indexer;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
@RunWith(Parameterized.class)
public class JavaScriptWorkerTest {

    private static final Logger log = LoggerFactory.getLogger(JavaScriptWorkerTest.class);

    private final String dir;
    private final File path;

    public JavaScriptWorkerTest(String dir, String path) {
        this.dir = dir;
        this.path = new File(path);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection getContent() throws Exception {
        ArrayList<String[]> list = new ArrayList<>();
        String name = JavaScriptWorkerTest.class.getSimpleName();
        ClassLoader classLoader = JavaScriptWorkerTest.class.getClassLoader();
        URL resource = classLoader.getResource(name);
        if (resource == null || !resource.getProtocol().equals("file")) {
            throw new Exception("Cannot find catalog for: " + name);
        }
        File file = new File(resource.getPath());
        if (!file.isDirectory()) {
            throw new Exception("Cannot find catalog for: " + name + " not a directory");
        }
        File[] dirs = file.listFiles(new FileFilter() {

            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        for (File dir : dirs) {
            list.add(new String[]{dir.getName(), dir.getPath()});
        }
        return list;
    }

    @Test
    public void testAddFields() throws Exception {
        String content = getContent(new File(path, "record"));
        String mimetype = getContent(new File(path, "mimetype")).trim();
        String expected = getContent(new File(path, "expected"));

        HashMap<String, HashSet<String>> collection = new HashMap<>();
        SolrInputDocument sid = mockSolrInputDocument(collection);

        JavaScriptWorker jsw = new JavaScriptWorker();

        jsw.addFields(sid, content, mimetype);

        validate(expected, collection);
    }

    private void validate(String expected, HashMap<String, HashSet<String>> actual) {
        HashMap<String, HashSet<String>> missing = new HashMap<>();
        JsonReader reader = Json.createReader(new StringReader(expected));
        JsonObject obj = reader.readObject();
        for (String key : obj.keySet()) {
            HashSet<String> set = actual.get(key);
            if (set == null) {
                HashSet<String> more = new HashSet<>();
                missing.put(key, more);
                JsonArray array = obj.getJsonArray(key);
                for (int i = 0 ; i < array.size() ; i++) {
                    more.add(array.getString(i));
                }
            } else {
                HashSet<String> more = new HashSet<>();
                JsonArray array = obj.getJsonArray(key);
                for (int i = 0 ; i < array.size() ; i++) {
                    String string = array.getString(i);
                    if (!set.remove(string)) {
                        more.add(string);
                    }
                }
                if (!more.isEmpty()) {
                    missing.put(key, more);
                }
                if (set.isEmpty()) {
                    actual.remove(key);
                }
            }
        }
        if(actual.isEmpty() && missing.isEmpty())
            return;
        StringBuilder sb = new StringBuilder();
        sb.append("Content error: ");
        if(!actual.isEmpty())
            sb.append("has extra: ").append(actual);
        if(!actual.isEmpty() && !missing.isEmpty())
            sb.append(" ");
        if(!missing.isEmpty())
            sb.append("missing: ").append(missing);
        fail(sb.toString());
    }

    private SolrInputDocument mockSolrInputDocument(final HashMap<String, HashSet<String>> collection) {
        SolrInputDocument sid = mock(SolrInputDocument.class);
        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                String name = (String) arguments[0];
                String value = (String) arguments[1];
                log.debug("mock: name = " + name + "; value = " + value);
                HashSet<String> set = collection.get(name);
                if (set == null) {
                    set = new HashSet<>();
                    collection.put(name, set);
                }
                set.add(value);
                return null;
            }
        }).when(sid).addField(anyString(), anyString());
        return sid;
    }

    private String getContent(File file) throws MalformedURLException, IOException {
        InputStream stream = file.toURI().toURL().openStream();
        int available = stream.available();
        byte[] bytes = new byte[available];
        stream.read(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
