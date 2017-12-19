/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.stats;

public class RecordStats {

    private int agencyId;
    private int marcxCount;
    private int enrichmentCount;

    public RecordStats() {
    }

    public RecordStats(int agencyId, int marcxCount, int enrichmentCount) {
        this.agencyId = agencyId;
        this.marcxCount = marcxCount;
        this.enrichmentCount = enrichmentCount;
    }

    public int getAgencyId() {
        return agencyId;
    }

    public void setAgencyId(int agencyid) {
        this.agencyId = agencyid;
    }

    public int getMarcxCount() {
        return marcxCount;
    }

    public void setMarcxCount(int marcxCount) {
        this.marcxCount = marcxCount;
    }

    public int getEnrichmentCount() {
        return enrichmentCount;
    }

    public void setEnrichmentCount(int enrichmentCount) {
        this.enrichmentCount = enrichmentCount;
    }

    @Override
    public String toString() {
        return "RecordStats{" +
                "agencyId=" + agencyId +
                ", marcxCount=" + marcxCount +
                ", enrichmentCount=" + enrichmentCount +
                '}';
    }
}
