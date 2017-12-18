/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

public class QueueProcessResponse {

    private boolean validated;
    private String message;

    public QueueProcessResponse() {
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
        return "QueueProcessResponse{" +
                "validated=" + validated +
                ", message='" + message + '\'' +
                '}';
    }
}
