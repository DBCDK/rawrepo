/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.common;

import java.util.Map;
import java.util.Properties;

/*
    This class is responsible for extracting the needed properties out of System.getEnv()
 */
public class PropertiesHelper {
    public static final String RAWREPO_URL = "RAWREPO_URL";
    public static final String RAWREPO_USER = "RAWREPO_USER";
    public static final String RAWREPO_PASS = "RAWREPO_PASS";

    public static final String HOLDINGS_ITEMS_URL = "HOLDINGS_ITEMS_URL";
    public static final String HOLDINGS_ITEMS_USER = "HOLDINGS_ITEMS_USER";
    public static final String HOLDINGS_ITEMS_PASS = "HOLDINGS_ITEMS_PASS";

    public static final String OPENAGENCY_URL = "OPENAGENCY_URL";
    public static final String OPENAGENCY_CACHE_AGE = "OPENAGENCY_CACHE_AGE";
    public static final String OPENAGENCY_CONNECT_TIMEOUT = "OPENAGENCY_CONNECT_TIMEOUT";
    public static final String OPENAGENCY_REQUEST_TIMEOUT = "OPENAGENCY_REQUEST_TIMEOUT";

    public static final String INSTANCE_NAME = "INSTANCE_NAME";

    // The reason this function takes System.getEnv() as argument instead of just calling the function is because of testability
    public static Properties getProperties(Map<String, String> systemProperties) throws NullPointerException {
        Properties properties = new Properties();

        String rawRepoURL = getEnvValue(systemProperties, RAWREPO_URL);
        if (rawRepoURL == null) {
            throw new NullPointerException("RAWREPO_URL is required and must have a value");
        } else {
            properties.setProperty(RAWREPO_URL, rawRepoURL);
        }

        String rawRepoUser = getEnvValue(systemProperties, RAWREPO_USER);
        if (rawRepoUser == null) {
            throw new NullPointerException("RAWREPO_USER is required and must have a value");
        } else {
            properties.setProperty(RAWREPO_USER, rawRepoUser);
        }

        String rawRepoPass = getEnvValue(systemProperties, RAWREPO_PASS);
        if (rawRepoPass == null) {
            throw new NullPointerException("RAWREPO_PASS is required and must have a value");
        } else {
            properties.setProperty(RAWREPO_PASS, rawRepoPass);
        }

        String holdingsURL = getEnvValue(systemProperties, HOLDINGS_ITEMS_URL);
        if (holdingsURL == null) {
            throw new NullPointerException("HOLDINGS_ITEMS_URL is required and must have a value");
        } else {
            properties.setProperty(HOLDINGS_ITEMS_URL, holdingsURL);
        }

        String holdingsUser = getEnvValue(systemProperties, HOLDINGS_ITEMS_USER);
        if (holdingsUser == null) {
            throw new NullPointerException("HOLDINGS_ITEMS_USER is required and must have a value");
        } else {
            properties.setProperty(HOLDINGS_ITEMS_USER, holdingsUser);
        }

        String holdingsPass = getEnvValue(systemProperties, HOLDINGS_ITEMS_PASS);
        if (holdingsPass == null) {
            throw new NullPointerException("HOLDINGS_ITEMS_PASS is required and must have a value");
        } else {
            properties.setProperty(HOLDINGS_ITEMS_PASS, holdingsPass);
        }

        String openAgencyUrl = getEnvValue(systemProperties, OPENAGENCY_URL);
        if (openAgencyUrl == null) {
            throw new NullPointerException("OPENAGENCY_URL is required and must have a value");
        } else {
            properties.setProperty(OPENAGENCY_URL, openAgencyUrl);
        }

        String openAgencyCacheAge = getEnvValue(systemProperties, OPENAGENCY_CACHE_AGE);
        if (openAgencyCacheAge == null) {
            properties.setProperty(OPENAGENCY_CACHE_AGE, ApplicationConstants.OPENAGENCY_CACHE_AGE.toString());
        } else {
            properties.setProperty(OPENAGENCY_CACHE_AGE, openAgencyCacheAge);
        }

        String openAgencyConnectTimeout = getEnvValue(systemProperties, OPENAGENCY_CONNECT_TIMEOUT);
        if (openAgencyConnectTimeout == null) {
            properties.setProperty(OPENAGENCY_CONNECT_TIMEOUT, ApplicationConstants.OPENAGENCY_CONNECT_TIMEOUT.toString());
        } else {
            properties.setProperty(OPENAGENCY_CONNECT_TIMEOUT, openAgencyConnectTimeout);
        }

        String openAgencyRequestTimeout = getEnvValue(systemProperties, OPENAGENCY_REQUEST_TIMEOUT);
        if (openAgencyRequestTimeout == null) {
            properties.setProperty(OPENAGENCY_REQUEST_TIMEOUT, ApplicationConstants.OPENAGENCY_REQUEST_TIMEOUT.toString());
        } else {
            properties.setProperty(OPENAGENCY_REQUEST_TIMEOUT, openAgencyRequestTimeout);
        }

        String instanceName = getEnvValue(systemProperties, INSTANCE_NAME);
        if (instanceName == null) {
            properties.setProperty(INSTANCE_NAME, "Ukendt");
        } else {
            properties.setProperty(INSTANCE_NAME, instanceName);
        }

        return properties;
    }

    private static String getEnvValue(Map<String, String> systemProperties, String key) {
        String result;

        try {
            result = systemProperties.get(key);
        } catch (NullPointerException ex) {
            return null;
        }

        if (result == null || result.isEmpty()) {
            return null;
        }

        return result;
    }
}
