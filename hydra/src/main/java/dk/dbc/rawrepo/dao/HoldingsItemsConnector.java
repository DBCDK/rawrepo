/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.holdingsitems.HoldingsItemsDAO;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.timer.Stopwatch;
import dk.dbc.rawrepo.timer.StopwatchInterceptor;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class HoldingsItemsConnector {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(HoldingsItemsConnector.class);

    @Resource(lookup = "jdbc/holdingsitems")
    DataSource globalDataSource;

    @PostConstruct
    public void postConstruct() {
        LOGGER.debug("HoldingsItemsConnector.postConstruct()");

        if (!healthCheck()) {
            throw new RuntimeException("Unable to connection to Holdings Items"); // Can't throw checked exceptions from postConstruct
        }
    }

    public boolean healthCheck() {
        try (Connection connection = globalDataSource.getConnection()) {
            try (CallableStatement stmt = connection.prepareCall("SELECT 1")) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    resultSet.next();

                    return true;
                }
            }
        } catch (SQLException ex) {
            return false;
        }
    }


    @Stopwatch
    public Set<RecordId> getHoldingsRecords(Set<Integer> agencies) throws Exception {
        LOGGER.entry(agencies);
        Set<RecordId> result = new HashSet<>();
        try (Connection connection = globalDataSource.getConnection()) {
            for (Integer agencyId : agencies) {
                HoldingsItemsDAO holdingsItemsDAO = HoldingsItemsDAO.newInstance(connection);
                Set<String> bibliographicRecordIds = holdingsItemsDAO.getBibliographicIds(agencyId);

                for (String bibliographicRecordId : bibliographicRecordIds) {
                    result.add(new RecordId(bibliographicRecordId, agencyId));
                }
            }

            return result;
        } finally {
            LOGGER.exit(result);
        }
    }
}