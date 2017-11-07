package dk.dbc.rawrepo.json;

import java.util.Set;

public class QueueValidateRequest {

    private QueueType queueType;
    private String provider;
    private Set<Integer> agencyIds;
    private boolean includeDeleted;
    private Integer chunk;
    private String sessionId;

    public QueueValidateRequest() {

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

    public Integer getChunk() {
        return chunk;
    }

    public void setChunk(Integer chunk) {
        this.chunk = chunk;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String toString() {
        return "QueueValidateRequest{" +
                "queueType=" + queueType.toString() +
                ", provider='" + provider + '\'' +
                ", agencyIds=" + agencyIds +
                ", includeDeleted=" + includeDeleted +
                ", chunk=" + chunk +
                ", sessionId='" + sessionId + '\'' +
                '}';
    }
}
