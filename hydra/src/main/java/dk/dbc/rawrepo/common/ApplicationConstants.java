/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.common;

public class ApplicationConstants {

    public static final String API_QUEUE = "/queue";
    public static final String API_QUEUE_ENQUEUE = "enqueue";
    public static final String API_QUEUE_PROVIDERS = "providers";
    public static final String API_QUEUE_PROVIDER_INFO = "providerInfo";

    public static final String API_LIBRARY = "/library";
    public static final String API_LIBRARY_QUEUE_TYPES = "queueTypes";
    public static final String API_LIBRARY_CATALOGING_TEMPLATE_SET = "catalogingTemplateSet";

    public static final String API_HYDRA = "/hydra";
    public static final String API_HYDRA_STATUS = "status";
    public static final String API_HYDRA_INSTANCE_NAME = "instance";

    public static final String OPENAGENCY_URL = "OPENAGENCY_URL";
    public static final String OPENAGENCY_CACHE_AGE = "OPENAGENCY_CACHE_AGE";
    public static final String OPENAGENCY_CACHE_AGE_DEFAULT = String.valueOf(8);
    public static final String OPENAGENCY_CONNECT_TIMEOUT = "OPENAGENCY_CONNECT_TIMEOUT";
    public static final String OPENAGENCY_CONNECT_TIMEOUT_DEFAULT = String.valueOf(60 * 1000);
    public static final String OPENAGENCY_REQUEST_TIMEOUT = "OPENAGENCY_REQUEST_TIMEOUT";
    public static final String OPENAGENCY_REQUEST_TIMEOUT_DEFAULT = String.valueOf(3 * 60 * 1000);

    public static final String RAWREPO_URL = "RAWREPO_URL";
    public static final String RAWREPO_USER = "RAWREPO_USER";
    public static final String RAWREPO_PASS = "RAWREPO_PASS";

    public static final String HOLDINGS_ITEMS_URL = "HOLDINGS_ITEMS_URL";
    public static final String HOLDINGS_ITEMS_USER = "HOLDINGS_ITEMS_USER";
    public static final String HOLDINGS_ITEMS_PASS = "HOLDINGS_ITEMS_PASS";

    public static final String INSTANCE_NAME = "INSTANCE_NAME";
}
