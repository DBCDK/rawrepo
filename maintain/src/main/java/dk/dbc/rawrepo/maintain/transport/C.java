package dk.dbc.rawrepo.maintain.transport;

/**
 *
 * @author bogeskov
 */
public class C {

    public static final String NS = "http://rawrepo.dbc.dk/maintain/";
    public static final String SERVICE = "Maintain";
    public static final String VERSION = "1.0";

    public static final String PORT = SERVICE + "/" + VERSION;
    public static final String ACTION_PATH = NS + SERVICE + "/";

    public static final String DATASOURCE = "jdbc/rawrepomaintain/rawrepo";
    public static final String PROPERTIES = "rawrepo-maintain";

    public static final String OPERATION_PAGE_CONTENT = "pageContent";
    public static final String OPERATION_QUEUE_RECORDS = "queueRecords";
    public static final String OPERATION_REMOVE_RECORDS = "removeRecords";
    public static final String OPERATION_REVERT_RECORDS = "revertRecords";
}
