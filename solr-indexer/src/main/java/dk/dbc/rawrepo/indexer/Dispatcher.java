package dk.dbc.rawrepo.indexer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import javax.enterprise.concurrent.ManagedExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Resource(name = "timeout")
    int timeout;
    @Resource(name = "maxConcurrent")
    int maxConcurrent;

    // all runnables that are given to "mes", they remove themselves upon completion
    private final Collection<Runnable> runnables = new HashSet<>();
    // all futures refering to a runnable that is given to "mes"
    private final Collection<Future> futures = new HashSet<>();

    private final TimerConfig timerConfig = new TimerConfig();
    private Timer timer = null;

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
                log.warn("IllegalState. timer doesn't exists ant stopTimer() was called");
            }
        }
    }

    /**
     * Starts the @Timeout timer
     */
    void startTimer(int initialdelaySeconds) {
        log.debug("startTimer() with {} seconds initial delay and {} seconds timeout", initialdelaySeconds, timeout);
        synchronized (timerConfig) {
            if (timer != null) {
                log.warn("IllegalState. timer exists but startTimer() was called");
                timer.cancel();
                timer = null;
            }
            timer = timerService.createIntervalTimer(initialdelaySeconds*1000, timeout * 1000, timerConfig);
        }
    }

    /**
     * Adds the runnable to the collection if currently running runnables
     *
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
     *
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
        for (Iterator<Future> i = futures.iterator(); i.hasNext();) {
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
        indexer.performWork();
    }

}
