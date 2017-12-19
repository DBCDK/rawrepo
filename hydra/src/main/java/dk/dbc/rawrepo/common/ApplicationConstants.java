/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.common;

public class ApplicationConstants {

    public static final String API_QUEUE = "/queue";
    public static final String API_QUEUE_VALIDATE = "validate";
    public static final String API_QUEUE_PROCESS = "process";
    public static final String API_QUEUE_TYPES = "types";
    public static final String API_QUEUE_PROVIDERS = "providers";

    public static final String API_LIBRARY = "/library";
    public static final String API_LIBRARY_CATALOGING_TEMPLATE_SET = "catalogingTemplateSet";

    public static final String API_HYDRA = "/hydra";
    public static final String API_HYDRA_STATUS = "status";
    public static final String API_HYDRA_INSTANCE_NAME = "instance";

    public static final String API_STATS = "/stats";
    public static final String API_STATS_RECORDS = "/recordByAgency";
    public static final String API_STATS_QUEUE_AGENCIES = "/queueByAgency";
    public static final String API_STATS_QUEUE_WORKERS = "/queueByWorker";

    public static final String OPENAGENCY_URL = "OPENAGENCY_URL";
    public static final String OPENAGENCY_CACHE_AGE = "OPENAGENCY_CACHE_AGE";
    public static final String OPENAGENCY_CACHE_AGE_DEFAULT = String.valueOf(8); // 8 hours
    public static final String OPENAGENCY_CONNECT_TIMEOUT = "OPENAGENCY_CONNECT_TIMEOUT";
    public static final String OPENAGENCY_CONNECT_TIMEOUT_DEFAULT = String.valueOf(60 * 1000); // One minute
    public static final String OPENAGENCY_REQUEST_TIMEOUT = "OPENAGENCY_REQUEST_TIMEOUT";
    public static final String OPENAGENCY_REQUEST_TIMEOUT_DEFAULT = String.valueOf(3 * 60 * 1000); // Three minutes

    public static final String INSTANCE_NAME = "INSTANCE_NAME";
}
