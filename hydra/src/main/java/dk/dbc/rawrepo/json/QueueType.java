/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.json;

public class QueueType {

    private String key;
    private String description;

    public static final String KEY_FFU = "ffu";
    public static final String KEY_FBS_RR = "fbs_rr";
    public static final String KEY_FBS_HOLDINGS = "fbs_holdings";

    private QueueType(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public static QueueType ffu() {
        return new QueueType(KEY_FFU, "FFU - lokalposter");
    }

    public static QueueType fbsRawrepo() {
        return new QueueType(KEY_FBS_RR, "FBS - Rawrepo");
    }

    public static QueueType fbsHoldings() {
        return new QueueType(KEY_FBS_HOLDINGS, "FBS - Beholdning");
    }

    public static QueueType fromString(String key) {
        switch (key) {
            case (KEY_FFU):
                return ffu();
            case (KEY_FBS_RR):
                return fbsRawrepo();
            case (KEY_FBS_HOLDINGS):
                return fbsHoldings();
            default:
                return null;
        }
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }

    public String getCatalogingTemplateSet() {
        switch (key) {
            case (KEY_FFU):
                return "ffu";
            case (KEY_FBS_RR):
            case (KEY_FBS_HOLDINGS):
                return "fbs";
            default:
                return null; // Should never happen
        }
    }

}
