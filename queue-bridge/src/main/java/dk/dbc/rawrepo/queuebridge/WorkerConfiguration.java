/*
 * Copyright (C) 2017 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-queue-bridge
 *
 * dbc-rawrepo-queue-bridge is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-queue-bridge is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.queuebridge;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class WorkerConfiguration {

    @NotNull
    private String queueServer;

    public String getQueueServer() {
        return queueServer;
    }

    public void setQueueServer(String queueServer) {
        this.queueServer = queueServer;
    }

    @NotNull
    @Min(1)
    private long pollInterval;

    public long getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(long pollInterval) {
        this.pollInterval = pollInterval;
    }

    @NotNull
    @Min(1)
    private int dbToMqThreads;

    public int getDbToMqThreads() {
        return dbToMqThreads;
    }

    public void setDbToMqThreads(int dbToMqThreads) {
        this.dbToMqThreads = dbToMqThreads;
    }

    @NotNull
    @Min(1)
    private int mqToDbThreads;

    public int getMqToDbThreads() {
        return mqToDbThreads;
    }

    public void setMqToDbThreads(int mqToDbThreads) {
        this.mqToDbThreads = mqToDbThreads;
    }

    @NotNull
    private String dbToMqQueues;

    public String getDbToMqQueues() {
        return dbToMqQueues;
    }

    public void setDbToMqQueues(String dbToMqQueues) {
        this.dbToMqQueues = dbToMqQueues;
    }

    @NotNull
    private String mqToDbQueues;

    public String getMqToDbQueues() {
        return mqToDbQueues;
    }

    public void setMqToDbQueues(String mqToDbQueues) {
        this.mqToDbQueues = mqToDbQueues;
    }

}
