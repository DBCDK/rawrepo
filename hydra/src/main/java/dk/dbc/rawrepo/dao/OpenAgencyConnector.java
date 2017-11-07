/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.json.QueueType;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Stateless
public class OpenAgencyConnector {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(OpenAgencyConnector.class);

    private OpenAgencyServiceFromURL service;

    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();
        StopWatch watch = new Log4JStopWatch("service.openagency.init");

        if (!System.getenv().containsKey(ApplicationConstants.OPENAGENCY_URL)) {
            throw new RuntimeException("OPENAGENCY_URL must have a value");
        }

        try {
            OpenAgencyServiceFromURL.Builder builder = OpenAgencyServiceFromURL.builder();
            builder = builder.
                    connectTimeout(Integer.parseInt(System.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_CONNECT_TIMEOUT,
                            ApplicationConstants.OPENAGENCY_CONNECT_TIMEOUT_DEFAULT))).

                    requestTimeout(Integer.parseInt(System.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_REQUEST_TIMEOUT,
                            ApplicationConstants.OPENAGENCY_REQUEST_TIMEOUT_DEFAULT))).

                    setCacheAge(Integer.parseInt(System.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_CACHE_AGE,
                            ApplicationConstants.OPENAGENCY_CACHE_AGE_DEFAULT)));

            service = builder.build(System.getenv().get(ApplicationConstants.OPENAGENCY_URL));
        } finally {
            watch.stop();
            LOGGER.exit();
        }

    }

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
