/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

public class AgencyAnalysis {

    private int agencyId;
    private int count;

    public AgencyAnalysis() {
    }

    public AgencyAnalysis(int agencyId, int count) {
        this.agencyId = agencyId;
        this.count = count;
    }

    public int getAgencyId() {
        return agencyId;
    }

    public void setAgencyId(int agencyId) {
        this.agencyId = agencyId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AgencyAnalysis{" +
                "agencyId=" + agencyId +
                ", count=" + count +
                '}';
    }
}
