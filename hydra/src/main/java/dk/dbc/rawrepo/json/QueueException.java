package dk.dbc.rawrepo.json;

public class QueueException extends Exception {

    public QueueException() {
    }

    public QueueException(String message) {
        super(message);
    }

    public QueueException(Throwable cause) {
        super(cause);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }

}
