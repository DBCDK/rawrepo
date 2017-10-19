/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.timer.Stopwatch;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Stateless;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@Singleton
@Stateless
public class HoldingsItemsDAO {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(HoldingsItemsDAO.class);

    private static Connection holdingsItemsConnection;

    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();
        StopWatch watch = new Log4JStopWatch("service.openagency.init");

        checkProperties();

        try {
            LOGGER.info("Connecting to Holdings Items URL {}", System.getenv().get(ApplicationConstants.HOLDINGS_ITEMS_URL));
            holdingsItemsConnection = DriverManager.getConnection(System.getenv().get(ApplicationConstants.HOLDINGS_ITEMS_URL),
                    System.getenv().get(ApplicationConstants.HOLDINGS_ITEMS_USER),
                    System.getenv().get(ApplicationConstants.HOLDINGS_ITEMS_PASS));
            holdingsItemsConnection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            watch.stop();
            LOGGER.exit();
        }

    }

    private void checkProperties() {
        if (!System.getenv().containsKey(ApplicationConstants.HOLDINGS_ITEMS_URL)) {
            throw new RuntimeException("HOLDINGS_ITEMS_URL must have a value");
        }

        if (!System.getenv().containsKey(ApplicationConstants.HOLDINGS_ITEMS_USER)) {
            throw new RuntimeException("HOLDINGS_ITEMS_USER must have a value");
        }

        if (!System.getenv().containsKey(ApplicationConstants.HOLDINGS_ITEMS_PASS)) {
            throw new RuntimeException("HOLDINGS_ITEMS_PASS must have a value");
        }
    }

    @Stopwatch
    public HashMap<Integer, Set<String>> getHoldingsRecords(List<Integer> agencies) throws Exception {
        LOGGER.entry(agencies);
        HashMap<Integer, Set<String>> result = new HashMap<>();
        try {
            for (Integer agencyId : agencies) {
                dk.dbc.holdingsitems.HoldingsItemsDAO holdingsItemsDAO = dk.dbc.holdingsitems.HoldingsItemsDAO.newInstance(holdingsItemsConnection);
                result.put(agencyId, holdingsItemsDAO.getBibliographicIds(agencyId));
            }

            return result;
        } finally {
            LOGGER.exit(result);
        }
    }
}