package dk.dbc.rawrepo.json;

import java.util.Set;

public class QueueRequest {

    private QueueType queueType;
    private String provider;
    private Set<Integer> agencyIds;
    private boolean includeDeleted;
    private String chunk;

    public QueueRequest() {

    }

    public QueueType getQueueType() {
        return queueType;
    }

    public void setQueueType(QueueType queueType) {
        this.queueType = queueType;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public Set<Integer> getAgencyIds() {
        return agencyIds;
    }

    public void setAgencyIds(Set<Integer> agencyIds) {
        this.agencyIds = agencyIds;
    }

    public boolean isIncludeDeleted() {
        return includeDeleted;
    }

    public void setIncludeDeleted(boolean includeDeleted) {
        this.includeDeleted = includeDeleted;
    }

    public String getChunk() {
        return chunk;
    }

    public void setChunk(String chunk) {
        this.chunk = chunk;
    }
}
