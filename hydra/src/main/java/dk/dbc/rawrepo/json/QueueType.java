/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.json;

public class QueueType {

    private String key;
    private String description;
    private String catalogingTemplateSet;
    private boolean changed;
    private boolean leaf;

    public static final String KEY_FFU = "ffu";
    public static final String KEY_FBS_RR = "fbs_rr";
    public static final String KEY_FBS_HOLDINGS = "fbs_holdings";

    private QueueType(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public static QueueType ffu() {
        QueueType queueType = new QueueType(KEY_FFU, "FFU - lokalposter");
        queueType.catalogingTemplateSet = "ffu";
        queueType.changed = true;
        queueType.leaf = false;

        return queueType;
    }

    public static QueueType fbsRawrepo() {
        QueueType queueType =  new QueueType(KEY_FBS_RR, "FBS - Rawrepo");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = false;

        return queueType;
    }

    public static QueueType fbsHoldings() {
        QueueType queueType =  new QueueType(KEY_FBS_HOLDINGS, "FBS - Beholdning");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = false;
        queueType.leaf = true;

        return queueType;
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
        return catalogingTemplateSet;
    }

    public boolean isChanged() {
        return changed;
    }

    public boolean isLeaf() {
        return leaf;
    }
}
