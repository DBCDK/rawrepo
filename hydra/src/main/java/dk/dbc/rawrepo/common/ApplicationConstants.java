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
    public static final String API_LIBRARY_CATALOGING_TEMPLATE_SETS = "catalogingTemplateSets";
    public static final String API_LIBRARY_CATALOGING_TEMPLATE_SET = "catalogingTemplateSet";

    public static final Integer OPENAGENCY_CACHE_AGE = 8;
    public static final Integer OPENAGENCY_CONNECT_TIMEOUT = 60 * 1000;
    public static final Integer OPENAGENCY_REQUEST_TIMEOUT = 3 * 60 * 1000;
}
