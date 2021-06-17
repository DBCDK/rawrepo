/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.maintain;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RawRepoWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RawRepoWorker.class);

    private final DataSource dataSource;
    private final VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;
    private Connection connection;
    private RawRepoDAO dao;

    public RawRepoWorker(DataSource dataSource, VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector) {
        this.dataSource = dataSource;
        this.vipCoreLibraryRulesConnector = vipCoreLibraryRulesConnector;
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
                    dao = RawRepoDAO.builder(getConnection()).relationHints(new RelationHintsVipCore(vipCoreLibraryRulesConnector)).build();
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

    protected static Date UTC(Date date) {
        TimeZone tz = TimeZone.getDefault();
        int offset = tz.getRawOffset();
        if (tz.inDaylightTime(date)) {
            offset += tz.getDSTSavings();
        }
        return new Date(date.getTime() - offset);
    }

    protected static Date UTC() {
        return UTC(new Date());
    }

}
