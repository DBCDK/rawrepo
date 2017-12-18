/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

public class QueueValidateRequest {

    private String queueType;
    private String provider;
    private String agencyText;
    private boolean includeDeleted;

    public QueueValidateRequest() {

    }

    public String getQueueType() {
        return queueType;
    }

    public void setQueueType(String queueType) {
        this.queueType = queueType;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getAgencyText() {
        return agencyText;
    }

    public void setAgencyText(String agencyText) {
        this.agencyText = agencyText;
    }

    public boolean isIncludeDeleted() {
        return includeDeleted;
    }

    public void setIncludeDeleted(boolean includeDeleted) {
        this.includeDeleted = includeDeleted;
    }

    @Override
    public String toString() {
        return "QueueValidateRequest{" +
                "queueType='" + queueType + '\'' +
                ", provider='" + provider + '\'' +
                ", agencyText='" + agencyText + '\'' +
                ", includeDeleted=" + includeDeleted +
                '}';
    }
}
