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
    public static final String KEY_FBS_RR_ENRICHEMENT = "fbs_rr_enrich";
    public static final String KEY_FBS_HOLDINGS = "fbs_holdings";
    public static final String KEY_FBS_EVERYTHING = "fbs_everything";

    private QueueType(String key, String description) {
        this.key = key;
        this.description = description;
    }

    /*
    * This is not 100% accurate but sort of understandable:
    * changed = true means Rawrepo Solr is updated.
    * changed = false means Rawrepo Solr is not updated
    * leaf = true means dataio job is created and Corepo Solr is updated
    * leaf = false means nothing is sent to dataio and Corepo
    *
    * BDM always want enqueuing to create a dataio job which in turn updates corepo. Therefor leaf is always true
    * At the same time we want a "marker" in rawrepo solr so we can see something has happend, there for changed = true
    * */

    public static QueueType ffu() {
        QueueType queueType = new QueueType(KEY_FFU, "FFU - RR Lokalposter");
        queueType.catalogingTemplateSet = "ffu";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsRawrepo() {
        QueueType queueType =  new QueueType(KEY_FBS_RR, "FBS - RR lokalposter + RR påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsRawrepoEnrichment() {
        QueueType queueType =  new QueueType(KEY_FBS_RR_ENRICHEMENT, "FBS - RR Påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsHoldings() {
        QueueType queueType =  new QueueType(KEY_FBS_HOLDINGS, "FBS - Beholdning");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsEverything() {
        QueueType queueType =  new QueueType(KEY_FBS_EVERYTHING, "FBS - Beholdning + RR lokalposter + RR påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fromString(String key) {
        switch (key) {
            case (KEY_FFU):
                return ffu();
            case (KEY_FBS_RR):
                return fbsRawrepo();
            case (KEY_FBS_RR_ENRICHEMENT):
                return fbsRawrepoEnrichment();
            case (KEY_FBS_HOLDINGS):
                return fbsHoldings();
            case (KEY_FBS_EVERYTHING):
                return fbsEverything();
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

    @Override
    public String toString() {
        return description;
    }
}
