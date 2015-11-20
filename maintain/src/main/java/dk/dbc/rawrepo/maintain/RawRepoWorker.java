/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-maintain
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class RawRepoWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RawRepoWorker.class);

    private final DataSource dataSource;
    private final OpenAgencyServiceFromURL openAgency;
    private final ExecutorService executorService;
    private Connection connection;
    private RawRepoDAO dao;

    public RawRepoWorker(DataSource dataSource, OpenAgencyServiceFromURL openAgency, ExecutorService executorService) {
        this.dataSource = dataSource;
        this.openAgency = openAgency;
        this.executorService = executorService;
        this.connection = null;
        this.dao = null;
    }

    public Connection getConnection() throws SQLException {
        synchronized (this) {
            if (connection == null) {
                connection = dataSource.getConnection();
            }
            return connection;
        }
    }

    public RawRepoDAO getDao() throws RawRepoException {
        try {
            synchronized (this) {
                if (dao == null) {
                    dao = RawRepoDAO.builder(getConnection()).openAgency(openAgency, executorService).build();
                }
                return dao;
            }
        } catch (SQLException ex) {
            throw new RawRepoException("Cannot access database", ex);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                try {
                    if (connection.getAutoCommit() == false) {
                        connection.rollback();
                    }
                } catch (SQLException ex) {
                    log.error("Cannot rollback connection " + ex.getMessage());
                }
                connection.close();
            } catch (SQLException ex) {
                log.error("Cannot close connection " + ex.getMessage());
            }
        }
    }

    /*
     *       __  __     __
     *      / / / /__  / /___  ___  __________
     *     / /_/ / _ \/ / __ \/ _ \/ ___/ ___/
     *    / __  /  __/ / /_/ /  __/ /  (__  )
     *   /_/ /_/\___/_/ .___/\___/_/  /____/
     *               /_/
     */
    protected ArrayList<String> getProviders() throws SQLException {
        ArrayList<String> agencies = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement("SELECT DISTINCT(provider) FROM queuerules ORDER BY provider")) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    agencies.add(resultSet.getString(1));
                }
            }
        }
        return agencies;
    }

}
