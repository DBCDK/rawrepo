/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.exception;

// The class is named as it is in order to avoid confusion with the existing RawRepoException
public class SolrIndexerRawRepoException extends Exception {

    public SolrIndexerRawRepoException() {
    }

    public SolrIndexerRawRepoException(String message) {
        super(message);
    }

    public SolrIndexerRawRepoException(Throwable cause) {
        super(cause);
    }

    public SolrIndexerRawRepoException(String message, Throwable cause) {
        super(message, cause);
    }

}
