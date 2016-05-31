/*
 * dbc-rawrepo-solr-indexer
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-solr-indexer.
 *
 * dbc-rawrepo-solr-indexer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-solr-indexer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-solr-indexer.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.indexer;

import dk.dbc.jslib.ClasspathSchemeHandler;
import dk.dbc.jslib.Environment;
import dk.dbc.jslib.ModuleHandler;
import dk.dbc.jslib.SchemeURI;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class JavaScriptWorker {

    private static final Logger log = LoggerFactory.getLogger(JavaScriptWorker.class);
    private static final String INDEXER_SCRIPT = "indexer.js";
    private static final String INDEXER_METHOD = "index";

    /**
     * Std search path
     */
    private static final String[] searchPaths = new String[]{
        "classpath:javascript/",
        "classpath:javascript/javacore/",
        "classpath:javascript/jscommon/config/",
        "classpath:javascript/jscommon/convert/",
        "classpath:javascript/jscommon/devel/",
        "classpath:javascript/jscommon/external/",
        "classpath:javascript/jscommon/io/",
        "classpath:javascript/jscommon/marc/",
        "classpath:javascript/jscommon/net/",
        "classpath:javascript/jscommon/system/",
        "classpath:javascript/jscommon/util/",
        "classpath:javascript/jscommon/xml/"
    };

    private final Environment environment;

    public JavaScriptWorker() {
        try {
            environment = new Environment();
            ModuleHandler mh = new ModuleHandler();
            mh.registerNonCompilableModule("Tables"); // Unlikely we need this module.

            // Builtin searchpath
            SolrFieldsSchemeHandler solrFields = new SolrFieldsSchemeHandler(this);
            mh.registerHandler(SolrFieldsSchemeHandler.SOLR_FIELDS_SCHEME, solrFields);
            mh.addSearchPath(new SchemeURI(SolrFieldsSchemeHandler.SOLR_FIELDS_SCHEME + ":"));

            // Classpath searchpath
            ClasspathSchemeHandler classpath = new ClasspathSchemeHandler(getClass().getClassLoader());
            mh.registerHandler("classpath", classpath);
            for (String searchPath : searchPaths) {
                mh.addSearchPath(new SchemeURI(searchPath));
            }
    //      mh.registerHandler("file", new FileSchemeHandler(root)); // Don'tuse filesystem

            // Use system
            environment.registerUseFunction(mh);

            // Evaluate script
            InputStream stream = getClass().getClassLoader().getResourceAsStream(INDEXER_SCRIPT);
            InputStreamReader inputStreamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        
            environment.eval(inputStreamReader, INDEXER_SCRIPT);
        } catch (Exception ex) {
            log.error("Error initializing javascript", ex);
            throw new RuntimeException("Cannot initlialize javascript", ex);
        }
    }

    /**
     * member variable exposed to javascript
     */
    private SolrInputDocument solrInputDocument;

    /**
     * JavaScript exposed method - adds field/value to solrInputDocument
     *
     * @param name
     * @param value
     */
    public void addField(String name, String value) {
        solrInputDocument.addField(name, value);
        log.trace("Adding " + name + ": " + value);
    }

    /**
     * Run script on content, adding data to solrInputDocument
     *
     * @param solrInputDocument target
     * @param content           String containing marcxchange
     * @param mimetype          mimetype of marcxchange
     * @throws Exception
     */
    void addFields(SolrInputDocument solrInputDocument, String content, String mimetype) throws Exception {
        this.solrInputDocument = solrInputDocument;

        environment.callMethod(INDEXER_METHOD, new Object[]{content, mimetype});
    }

}
