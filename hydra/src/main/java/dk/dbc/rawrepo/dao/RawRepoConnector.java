/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.json.QueueProvider;
import dk.dbc.rawrepo.json.QueueWorker;
import dk.dbc.rawrepo.timer.Stopwatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import java.sql.*;
import java.util.*;

@Stateless
public class RawRepoConnector {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoConnector.class);

    private static final String SELECT_QUEUERULES_ALL = "SELECT * FROM queuerules";
    private static final String SELECT_PROVIDERS = "SELECT DISTINCT(provider) FROM queuerules";
    private static final String CALL_ENQUEUE_BULK = "SELECT * FROM enqueue_bulk(?, ?, ?, ?, ?)";

    private static Connection rawRepoConnection;


    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();

        checkProperties();

        try {
            LOGGER.info("Connecting to Rawrepo URL {}", System.getenv().get(ApplicationConstants.RAWREPO_URL));
            rawRepoConnection = DriverManager.getConnection(System.getenv().get(ApplicationConstants.RAWREPO_URL),
                    System.getenv().get(ApplicationConstants.RAWREPO_USER),
                    System.getenv().get(ApplicationConstants.RAWREPO_PASS));
            rawRepoConnection.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new RuntimeException(ex); // Can't throw checked exceptions from postConstruct
        } finally {
            LOGGER.exit();
        }
    }

    private void checkProperties() {
        if (!System.getenv().containsKey(ApplicationConstants.RAWREPO_URL)) {
            throw new RuntimeException("OPENAGENCY_URL must have a value");
        }

        if (!System.getenv().containsKey(ApplicationConstants.RAWREPO_USER)) {
            throw new RuntimeException("RAWREPO_USER must have a value");
        }

        if (!System.getenv().containsKey(ApplicationConstants.RAWREPO_PASS)) {
            throw new RuntimeException("RAWREPO_PASS must have a value");
        }
    }

    @Stopwatch
    public List<String> getProviders() throws Exception {
        LOGGER.entry();
        List<String> result = new ArrayList<>();

        try (CallableStatement stmt = rawRepoConnection.prepareCall(SELECT_PROVIDERS)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("provider"));
                }
            }

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

    @Stopwatch
    public List<QueueProvider> getProviderDetails() throws Exception {
        LOGGER.entry();
        HashMap<String, QueueProvider> providerMap = new HashMap<>();
        List<QueueProvider> result = new ArrayList<>();

        try (CallableStatement stmt = rawRepoConnection.prepareCall(SELECT_QUEUERULES_ALL)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                QueueProvider provider;
                while (resultSet.next()) {
                    if (!providerMap.containsKey(resultSet.getString("provider"))) {
                        provider = new QueueProvider(resultSet.getString("provider"));
                        providerMap.put(provider.getName(), provider);
                    }

                    provider = providerMap.get(resultSet.getString("provider"));

                    QueueWorker worker = new QueueWorker(resultSet.getString("worker"),
                            resultSet.getString("changed").toUpperCase(),
                            resultSet.getString("leaf").toUpperCase());

                    provider.getWorkers().add(worker);
                }
            }

            for (String key : providerMap.keySet()) {
                result.add(providerMap.get(key));
            }

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

    @Stopwatch
    public Set<RecordId> getFFURecords(Set<Integer> agencies, Boolean includeDeleted) throws Exception {
        LOGGER.entry(agencies, includeDeleted);
        Set<RecordId> result = new HashSet<>();

        try {
            result = getRecordsForLibraries(agencies, includeDeleted);

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

    @Stopwatch
    public Set<RecordId> getFBSRecords(Set<Integer> agencies, Boolean includeDeleted) throws Exception {
        LOGGER.entry(agencies, includeDeleted);
        Set<RecordId> result = new HashSet<>();

        try {
            result = getRecordsForLibraries(agencies, includeDeleted);

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

    private Set<RecordId> getRecordsForLibraries(Set<Integer> agencies, boolean includeDeleted) throws Exception {
        LOGGER.entry(agencies, includeDeleted);
        Set<RecordId> result = new HashSet<>();

        // There is no smart or elegant way of doing a select 'in' clause, so this will have to do
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT bibliographicrecordid, agencyid FROM records WHERE agencyid IN (");
        for (int i = 0; i < agencies.size(); i++) {
            if (i > 0)
                sb.append(", ");
            sb.append("?");
        }
        sb.append(")");
        // By default deleted records are included with select *, so exclude them if they are not wanted
        if (!includeDeleted) {
            sb.append(" AND deleted = false");
        }

        String statement = sb.toString();

        try (CallableStatement stmt = rawRepoConnection.prepareCall(statement)) {
            int i = 1;
            for (Integer agencyId : agencies) {
                stmt.setInt(i++, agencyId);
            }

            LOGGER.debug("Executing statement: {}", statement);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String bibliographicRecordId = resultSet.getString("bibliographicrecordid");
                    final Integer agencyId = resultSet.getInt("agencyid");

                    result.add(new RecordId(bibliographicRecordId, agencyId));
                }
            }

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

    // Since a summary of the enqueuing have to be presented to the user we have to return the values somehow
    // This class only serves as a result container.
    public class EnqueueBulkResult {
        public String recordId;
        public Integer agencyId;
        public String worker;
        public Boolean queued;

        public EnqueueBulkResult(String recordId, Integer agencyId, String worker, Boolean queued) {
            this.recordId = recordId;
            this.agencyId = agencyId;
            this.worker = worker;
            this.queued = queued;
        }

        @Override
        public String toString() {
            return "EnqueueBulkResult{" +
                    "recordId='" + recordId + '\'' +
                    ", agencyId=" + agencyId +
                    ", worker='" + worker + '\'' +
                    ", queued=" + queued +
                    '}';
        }
    }

    @Stopwatch
    public List<EnqueueBulkResult> enqueueBulk(List<String> bibliographicRecordIdList,
                                               List<Integer> agencyList,
                                               List<String> providerList,
                                               List<Boolean> changedList,
                                               List<Boolean> leafList) throws Exception {
        LOGGER.entry();

        if (!(bibliographicRecordIdList.size() == agencyList.size() &&
                agencyList.size() == providerList.size() &&
                providerList.size() == changedList.size() &&
                changedList.size() == leafList.size())) {
            throw new Exception("All input list must have same size");
        }

        // Convert true/false to Y/N
        List<String> changedListChar = new ArrayList<>();
        List<String> leafListChar = new ArrayList<>();

        for (Boolean changed : changedList) {
            changedListChar.add(changed ? "Y" : "N");
        }

        for (Boolean leaf : leafList) {
            leafListChar.add(leaf ? "Y" : "N");
        }

        List<EnqueueBulkResult> result = new ArrayList<>();
        try (CallableStatement stmt = rawRepoConnection.prepareCall(CALL_ENQUEUE_BULK)) {
            stmt.setArray(1, stmt.getConnection().createArrayOf("VARCHAR", bibliographicRecordIdList.toArray(new String[bibliographicRecordIdList.size()])));
            stmt.setArray(2, stmt.getConnection().createArrayOf("NUMERIC", agencyList.toArray(new Integer[agencyList.size()])));
            stmt.setArray(3, stmt.getConnection().createArrayOf("VARCHAR", providerList.toArray(new String[providerList.size()])));
            stmt.setArray(4, stmt.getConnection().createArrayOf("VARCHAR", changedListChar.toArray(new String[changedListChar.size()])));
            stmt.setArray(5, stmt.getConnection().createArrayOf("VARCHAR", leafListChar.toArray(new String[leafListChar.size()])));

            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String recordId = resultSet.getString("bibliographicrecordid");
                    final Integer agencyId = resultSet.getInt("agencyid");
                    final String worker = resultSet.getString("worker");
                    final Boolean enqueued = resultSet.getString("queued").toUpperCase().equals("T");

                    result.add(new EnqueueBulkResult(recordId, agencyId, worker, enqueued));
                }
            }

            return result;
        } catch (SQLException ex) {
            throw new Exception(ex);
        } finally {
            LOGGER.exit(result);
        }
    }

}
