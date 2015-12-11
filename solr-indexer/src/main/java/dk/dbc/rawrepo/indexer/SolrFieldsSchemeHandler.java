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

import dk.dbc.jslib.Environment;
import dk.dbc.jslib.ISchemeHandler;
import dk.dbc.jslib.SchemeURI;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
class SolrFieldsSchemeHandler implements ISchemeHandler {

    private static final Logger log = LoggerFactory.getLogger(SolrFieldsSchemeHandler.class);

    private static final String content = getContent("core.js");
    private static final String SOLR_FIELDS = "SolrFields";
    static final String SOLR_FIELDS_SCHEME = "solrfieldsbuiltin";
    private static final List<String> SUPPORTED_SCHEMES = Collections.unmodifiableList(Arrays.asList(SOLR_FIELDS_SCHEME.split(":")));
    private final JavaScriptWorker javaScriptWorker;

    SolrFieldsSchemeHandler(JavaScriptWorker javaScriptWorker) {
        this.javaScriptWorker = javaScriptWorker;
    }

    @Override
    public List<String> schemes() {
        return SUPPORTED_SCHEMES;
    }

    @Override
    public SchemeURI lookup(SchemeURI suri, String string) {
        log.debug("Asking for: " + string);
        switch (string) {
            case SOLR_FIELDS:
                return new SchemeURI(suri.getScheme(), string);
            default:
                return new SchemeURI();
        }
    }

    @Override
    public void load ( SchemeURI sUri, Environment envir) throws Exception{
        switch (sUri.toString()) {
            case SOLR_FIELDS_SCHEME + "://" + SOLR_FIELDS:
                envir.put("__SolrFields", javaScriptWorker);
                envir.eval(content);
                break;
            default:
                throw new RuntimeException("Don't know how to load: " + sUri.toString());
        }
    }

    /**
     * Get file from classpath as string (UTF-8)
     *
     * @param filename
     * @return Content
     */
    private static String getContent(String filename) {
        Class<?> clazz = SolrFieldsSchemeHandler.class;
        
        InputStream stream = clazz.getClassLoader().getResourceAsStream(clazz.getCanonicalName() + "-" + filename);
        try {
            int available = stream.available();
            byte[] bytes = new byte[available];
            stream.read(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot read SolrFields", ex);
        }
    }

}
