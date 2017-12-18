/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

import dk.dbc.rawrepo.RecordId;

import java.util.Set;

public class QueueJob {

    private Set<RecordId> recordIdList;
    private Set<Integer> agencyIdList;
    private String provider;
    private QueueType queueType;
    private boolean includeDeleted;

    public QueueJob() {

    }

    public Set<RecordId> getRecordIdList() {
        return recordIdList;
    }

    public void setRecordIdList(Set<RecordId> recordIdList) {
        this.recordIdList = recordIdList;
    }

    public Set<Integer> getAgencyIdList() {
        return agencyIdList;
    }

    public void setAgencyIdList(Set<Integer> agencyIdList) {
        this.agencyIdList = agencyIdList;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public QueueType getQueueType() {
        return queueType;
    }

    public void setQueueType(QueueType queueType) {
        this.queueType = queueType;
    }

    public boolean isIncludeDeleted() {
        return includeDeleted;
    }

    public void setIncludeDeleted(boolean includeDeleted) {
        this.includeDeleted = includeDeleted;
    }

    @Override
    public String toString() {
        return "QueueJob{" +
                "recordIdList=" + recordIdList +
                ", agencyIdList=" + agencyIdList +
                ", provider='" + provider + '\'' +
                ", queueType=" + queueType.toString() +
                ", includeDeleted=" + includeDeleted +
                '}';
    }
}
