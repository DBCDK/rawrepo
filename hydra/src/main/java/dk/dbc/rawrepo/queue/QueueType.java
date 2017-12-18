/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

public class QueueType {

    private String key;
    private String description;
    private String catalogingTemplateSet;
    private boolean changed;
    private boolean leaf;

    public static final String FFU = "ffu";
    public static final String FBS_LOCAL = "fbs_rr";
    public static final String FBS_ENRICHMENT = "fbs_rr_enrich";
    public static final String FBS_HOLDINGS = "fbs_holdings";
    public static final String FBS_EVERYTHING = "fbs_everything";
    public static final String DBC_COMMON_ONLY = "dbc_common_only";

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
    * At the same time we want a "marker" in rawrepo solr so we can see something has happend, therefor changed = true
    */
    public static QueueType ffu() {
        QueueType queueType = new QueueType(FFU, "FFU - RR Lokalposter");
        queueType.catalogingTemplateSet = "ffu";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsRawrepo() {
        QueueType queueType =  new QueueType(FBS_LOCAL, "FBS - RR lokalposter + RR påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsRawrepoEnrichment() {
        QueueType queueType =  new QueueType(FBS_ENRICHMENT, "FBS - RR Påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsHoldings() {
        QueueType queueType =  new QueueType(FBS_HOLDINGS, "FBS - Beholdning");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType fbsEverything() {
        QueueType queueType =  new QueueType(FBS_EVERYTHING, "FBS - Beholdning + RR lokalposter + RR påhængsposter");

        queueType.catalogingTemplateSet = "fbs";
        queueType.changed = true;
        queueType.leaf = true;

        return queueType;
    }

    public static QueueType dbcCommon() {
        QueueType queueType =  new QueueType(DBC_COMMON_ONLY, "DBC - Fællesposter til RR solr (uden FBS påhæng)");

        queueType.catalogingTemplateSet = "dbc";
        queueType.changed = true;
        queueType.leaf = false;

        return queueType;
    }

    public static QueueType fromString(String key) {
        switch (key) {
            case (FFU):
                return ffu();
            case (FBS_LOCAL):
                return fbsRawrepo();
            case (FBS_ENRICHMENT):
                return fbsRawrepoEnrichment();
            case (FBS_HOLDINGS):
                return fbsHoldings();
            case (FBS_EVERYTHING):
                return fbsEverything();
            case (DBC_COMMON_ONLY):
                return dbcCommon();
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
        return key;
    }
}
