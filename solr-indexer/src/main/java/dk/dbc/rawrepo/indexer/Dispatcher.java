/*
 * dbc-rawrepo-solr-indexer
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-solr-indexer.
 *
 * dbc-rawrepo-solr-indexer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-solr-indexer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-solr-indexer.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.indexer;

import dk.dbc.eeconfig.EEConfig;
import dk.dbc.rawrepo.exception.SolrIndexerRawRepoException;
import dk.dbc.rawrepo.exception.SolrIndexerSolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.*;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.validation.constraints.Min;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Future;

@Singleton
@Startup
public class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    @Resource(/* name = "concurrent/myExecuter" */)
    ManagedExecutorService mes;

    @Resource
    TimerService timerService;

    @EJB
    Indexer indexer;

    @Inject
    @EEConfig.Name(C.TIMEOUT)
    @EEConfig.Default(C.TIMEOUT_DEFAULT)
    @Min(1)
    int timeout;

    @Inject
    @EEConfig.Name(C.MAX_CONCURRENT)
    @EEConfig.Default(C.MAX_CONCURRENT_DEFAULT)
    @Min(1)
    int maxConcurrent;

    // all runnables that are given to "mes", they remove themselves upon completion
    private final Collection<Runnable> runnables = new HashSet<>();
    // all futures refering to a runnable that is given to "mes"
    private final Collection<Future> futures = new HashSet<>();

    private final TimerConfig timerConfig = new TimerConfig();
    private Timer timer = null;

    private boolean executionError = false;

    /**
     * Stops the @Timeout timer
     */
    void stopTimer() {
        log.debug("stopTimer()");
        synchronized (timerConfig) {
            if (timer != null) {
                timer.cancel();
                timer = null;
            } else {
                log.warn("IllegalState. Timer doesn't exists and stopTimer() was called");
            }
        }
    }

    /**
     * Starts the @Timeout timer
     */
    void startTimer(int initialDelaySeconds) {
        log.debug("startTimer() with {} seconds initial delay and {} seconds timeout", initialDelaySeconds, timeout);
        synchronized (timerConfig) {
            if (timer != null) {
                log.warn("IllegalState. Timer exists but startTimer() was called");
                timer.cancel();
                timer = null;
            }
            timer = timerService.createIntervalTimer(initialDelaySeconds * 1000, timeout * 1000, timerConfig);
        }
    }

    /**
     * Adds the runnable to the collection if currently running runnables
     * <p>
     * If the collection has become full, stop the timer
     *
     * @param runnable
     */
    void addRunnable(Runnable runnable) {
        log.debug("addRunnable()");
        synchronized (runnables) {
            runnables.add(runnable);
            boolean isFull = runnables.size() == maxConcurrent;
            if (isFull) {
                stopTimer();
            }
        }
    }

    /**
     * Removes the runnable from the collection of currently running runnables
     * <p>
     * If the collection isn't full anymore start the timer
     *
     * @param runnable
     */
    void removeRunnable(Runnable runnable) {
        log.debug("removeRunnable()");
        synchronized (runnables) {
            boolean wasFull = runnables.size() == maxConcurrent;
            runnables.remove(runnable);
            boolean isFull = runnables.size() == maxConcurrent;

            if (executionError) {
                log.error("FATAL: Indexer has encountered and unrecoverable error and will now exit the program. Please fix the problem (see message above) and restart the indexer");
                System.exit(1);
            }

            if (!isFull && wasFull) {
                startTimer(timeout);
            }
        }
    }

    /**
     * Cleans up the collection of futures, removing any that is done
     */
    void cleanFutures() {
        log.debug("Cleaning up remaining workers");
        for (Iterator<Future> i = futures.iterator(); i.hasNext(); ) {
            Future future = i.next();
            if (future.isDone()) {
                i.remove();
            }
        }
    }

    /**
     * Setup timer
     */
    @PostConstruct
    public void setupTimer() {
        log.debug("setupTimer() @PostConstruct. timeout is {}, maxConcurrent is {}", timeout, maxConcurrent);
        timerConfig.setPersistent(false);
        startTimer(0);
    }

    /**
     * Find work and put it into a
     */
    @Timeout
    public void timeout() {
        log.debug("timeout() @Timeout");
        try {
            synchronized (this) {
                while (runnables.size() < maxConcurrent) {
                    cleanFutures();
                    log.debug("Creating new worker");
                    final Runnable runnable = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                performWork();
                            } catch (Exception ex) {
                                log.error("Work failed with", ex);
                            }
                            removeRunnable(this);
                        }
                    };

                    addRunnable(runnable);
                    Future future = mes.submit(runnable);
                    futures.add(future);
                }
            }
        } catch (Exception ex) {
            log.error("Caught unexpected (@Timeout)", ex);
        }
    }

    /**
     * Cancel all running Executors
     */
    @PreDestroy
    public void cancelAll() {
        log.debug("cancelAll() @PreDestroy");
        cleanFutures();
        for (Future<Void> future : futures) {
            future.cancel(true);
        }
    }

    /**
     * Make a callable, that should do something.
     *
     * @return
     */
    void performWork() {
        try {
            indexer.performWork();
        } catch (SolrIndexerRawRepoException ex) {
            log.error("SolrIndexerRawRepoException exception caught");
            log.error(ex.getMessage());
            executionError = true;
        } catch (SolrIndexerSolrException ex) {
            log.error("SolrIndexerSolrException exception caught");
            log.error(ex.getMessage());
            executionError = true;
        }
    }

}
