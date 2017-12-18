/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

import java.util.ArrayList;
import java.util.List;

public class QueueValidateResponse {

    private List<AgencyAnalysis> agencyAnalysisList;
    private int chunks;
    private String sessionId;
    private boolean validated;
    private String message;

    public QueueValidateResponse() {
        agencyAnalysisList = new ArrayList<>();
    }

    public List<AgencyAnalysis> getAgencyAnalysisList() {
        return agencyAnalysisList;
    }

    public void setAgencyAnalysisList(List<AgencyAnalysis> agencyAnalysisList) {
        this.agencyAnalysisList = agencyAnalysisList;
    }

    public int getChunks() {
        return chunks;
    }

    public void setChunks(int chunks) {
        this.chunks = chunks;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isValidated() {
        return validated;
    }

    public void setValidated(boolean validated) {
        this.validated = validated;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "QueueValidateResponse{" +
                "agencyAnalysisList=" + agencyAnalysisList.toString() +
                ", chunks=" + chunks +
                ", sessionId='" + sessionId + '\'' +
                ", validated=" + validated +
                ", message='" + message + '\'' +
                '}';
    }
}
