/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.common.PropertiesHelper;
import dk.dbc.rawrepo.timer.Stopwatch;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Stateless;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Singleton
@Stateless
public class OpenAgencyDAO {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(OpenAgencyDAO.class);

    private OpenAgencyServiceFromURL service;

    private Properties properties;

    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();
        StopWatch watch = new Log4JStopWatch("service.openagency.init");

        properties = PropertiesHelper.getProperties(System.getenv());

        try {
            OpenAgencyServiceFromURL.Builder builder = OpenAgencyServiceFromURL.builder();
            builder = builder.connectTimeout(Integer.parseInt(properties.getProperty(PropertiesHelper.OPENAGENCY_CONNECT_TIMEOUT))).
                    requestTimeout(Integer.parseInt(properties.getProperty(PropertiesHelper.OPENAGENCY_REQUEST_TIMEOUT))).
                    setCacheAge(Integer.parseInt(properties.getProperty(PropertiesHelper.OPENAGENCY_CACHE_AGE)));

            service = builder.build(properties.getProperty(PropertiesHelper.OPENAGENCY_URL));
        } finally {
            watch.stop();
            LOGGER.exit();
        }

    }

    public OpenAgencyServiceFromURL getService() {
        return service;
    }

    // TODO Implement properly!
    // This should preferable be a service call to openagency.
    // However since we are limiting the features right now this is simply a hardcoded list
    public List<String> getCatalogingTemplateSets() {
        return Arrays.asList("ffu", "fbs");
    }

    @Stopwatch
    public Set<String> getLibrariesByCatalogingTemplateSet(String template) throws OpenAgencyException {
        LOGGER.entry(template);
        StopWatch watch = new Log4JStopWatch("service.openagency.getLibrariesByCatalogingTemplateSet");

        try {
            return service.libraryRules().getLibrariesByCatalogingTemplateSet(template);
        } finally {
            watch.stop();
            LOGGER.exit();
        }
    }

}
