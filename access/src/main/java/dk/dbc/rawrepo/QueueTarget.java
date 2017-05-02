/*
 * Copyright (C) 2017 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public abstract class QueueTarget {

    private static final Logger log = LoggerFactory.getLogger(QueueTarget.class);

    public abstract void send(QueueJob job, List<String> queues) throws JMSException;

    /**
     * If commit is implemented, it's implementors job to collect all send jobs
     * and send them upon commit.
     *
     * @throws JMSException
     */
    public abstract void commit() throws JMSException;

    public static class Default extends QueueTarget {

        @Override
        public void send(QueueJob job, List<String> queues) throws JMSException {
            throw new JMSException("JMS is not initialized");
        }

        @Override
        public void commit() throws JMSException {
        }
    }

    public static class Mq extends QueueTarget {

        private final JMSContext context;
        private final HashMap<String, Queue> map;
        private final JMSProducer producer;
        private final HashMap<String, HashSet<QueueJob>> jobs;

        public Mq(JMSContext context) throws JMSException {
            this.context = context;
            this.producer = context.createProducer();
            this.map = new HashMap<>();
            this.jobs = new HashMap<>();
        }

        @Override
        public void send(QueueJob job, List<String> queues) throws JMSException {
            for (String queue : queues) {
                jobs.computeIfAbsent(queue, q -> new HashSet<>()).add(job);
            }
        }

        @Override
        public void commit() throws JMSException {
            try {
                for (Map.Entry<String, HashSet<QueueJob>> entry : jobs.entrySet()) {
                    Queue queue = map.computeIfAbsent(entry.getKey(), context::createQueue);
                    for (QueueJob job : entry.getValue()) {
                        Message message = job.jmsMessage(context);
                        producer.send(queue, message);
                    }
                }
                jobs.clear();
            } catch (RuntimeException ex) {
                Throwable cause = ex.getCause();
                if (cause != null && ( cause instanceof JMSException )) {
                    throw (JMSException) cause;
                }
                throw ex;
            }
        }
    }

    public static class OpenMq extends QueueTarget {

        private final JMSContext context;
        private final HashMap<String, Queue> map;
        private final JMSProducer producer;
        private final HashMap<String, HashSet<QueueJob>> jobs;
        private final int retryCount;
        private final int retryDelayMs;

        public OpenMq(JMSContext context, int retryCount, int retryDelayMs) throws JMSException {
            this.context = context;
            this.producer = context.createProducer();
            this.map = new HashMap<>();
            this.jobs = new HashMap<>();
            this.retryCount = retryCount;
            this.retryDelayMs = retryDelayMs;
        }

        @Override
        public void send(QueueJob job, List<String> queues) throws JMSException {
            for (String queue : queues) {
                jobs.computeIfAbsent(queue, q -> new HashSet<>()).add(job);
            }
        }

        @Override
        public void commit() throws JMSException {
            RuntimeException ex = null;

            for (int c = 0 ; c <= retryCount ; c++) {
                for (Iterator<Map.Entry<String, HashSet<QueueJob>>> ite = jobs.entrySet().iterator() ; ite.hasNext() ;) {
                    Map.Entry<String, HashSet<QueueJob>> entry = ite.next();
                    Queue queue = map.computeIfAbsent(entry.getKey(), context::createQueue);
                    HashSet<QueueJob> jobList = entry.getValue();
                    for (Iterator<QueueJob> ite2 = jobList.iterator() ; ite2.hasNext() ;) {
                        QueueJob job = ite2.next();
                        try {
                            Message message = job.jmsMessage(context);
                            producer.send(queue, message);
                            ite2.remove();
                        } catch (RuntimeException e) {
                            if (e.getClass().getCanonicalName().equals("com.sun.messaging.jms.MQRuntimeException")) {
                                ex = e;
                            } else {
                                throw ex;
                            }
                        }
                    }
                    if (jobList.isEmpty()) {
                        ite.remove();
                    }
                }
                if (jobs.isEmpty()) {
                    return;
                }
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    log.error("Error sleeping: " + e.getMessage());
                    log.debug("Error sleeping:", e);
                }
            }
            if (ex != null) {
                Throwable cause = ex.getCause();
                if (cause != null && ( cause instanceof JMSException )) {
                    throw (JMSException) cause;
                }
                throw ex;

            }
        }
    }
}
