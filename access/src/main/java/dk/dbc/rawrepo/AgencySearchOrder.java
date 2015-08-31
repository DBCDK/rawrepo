/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
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
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Interface for providing a list of agencies, which is the search order for
 * finding records for an agency
 * <p>
 * Implements caching, and threaded access, ensuring threadsafe access and
 * minimal list generation.
 * <p>
 * This ought to be in a @Singleton or an application global
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public abstract class AgencySearchOrder {

    private final HashMap<Integer, CacheEntry> cache;

    public AgencySearchOrder() {
        this.cache = new HashMap<>();
    }

    /**
     * Get a cached list of agencies for this agency
     *
     * @param agencyId
     * @return list in search order
     */
    public List<Integer> getAgenciesFor(int agencyId) {
        boolean fetch = false;
        CacheEntry cacheEntry;
        synchronized (cache) {
            cacheEntry = cache.get(agencyId);
            if (cacheEntry == null
                || cacheEntry.isTimeout()) {
                cacheEntry = new CacheEntry();
                cache.put(agencyId, cacheEntry);
                fetch = true;
            }
        }
        if (fetch) {
            cacheEntry.setList(provideAgenciesFor(agencyId));
        }
        return cacheEntry.getList();
    }

    /**
     *
     * @param agencyId
     * @return list of search order of agencies
     */
    public abstract List<Integer> provideAgenciesFor(int agencyId);

    private static final long MAX_AGE_SECONDS = 3600 * 12;

    private static class CacheEntry {

        private final Lock lock;
        private final Condition condition;

        private final long timeOut;
        private List<Integer> list;

        public CacheEntry() {
            this.lock = new ReentrantLock();
            this.condition = this.lock.newCondition();
            this.timeOut = System.currentTimeMillis() + MAX_AGE_SECONDS * 1000;
            this.list = null;
        }

        public boolean isTimeout() {
            return timeOut > System.currentTimeMillis();
        }

        public List<Integer> getList() {
            lock.lock();
            try {
                while (list == null) {
                    try {
                        condition.await();
                    } catch (InterruptedException ex) {
                    }
                }
            } finally {
                lock.unlock();
            }
            return list;
        }

        public void setList(List<Integer> list) {
            lock.lock();
            try {
                this.list = list;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

    }

}
