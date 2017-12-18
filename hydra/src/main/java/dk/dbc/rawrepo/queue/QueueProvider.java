/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.queue;

import java.util.ArrayList;
import java.util.List;

public class QueueProvider {

    private String name;
    private List<QueueWorker> workers;

    public QueueProvider(String name) {
        this.name = name;
        this.workers = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public List<QueueWorker> getWorkers() {
        return workers;
    }

    @Override
    public String toString() {
        return name;
    }

}
