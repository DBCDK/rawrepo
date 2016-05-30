/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import dk.dbc.gracefulcache.CacheProvider;
import dk.dbc.gracefulcache.CacheTimeoutException;
import dk.dbc.gracefulcache.CacheValueException;
import dk.dbc.gracefulcache.GracefulCache;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Interface for providing a list of agencies, which is the search order for
 * finding records for an agency
 * <p>
 * Implements caching, and threaded access, ensuring threadsafe access and
 * minimal list generation.
 * <p>
 * This ought to be in a @Singleton or an application global
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public abstract class AgencySearchOrder implements CacheProvider<Integer, List<Integer>> {

    private final GracefulCache<Integer, List<Integer>> cache;

    @SuppressWarnings("LeakingThisInConstructor")
    public AgencySearchOrder(ExecutorService es) {
        this.cache = new GracefulCache<>(this, es);
    }

    /**
     * Get a cached list of agencies for this agency
     *
     * @param agencyId
     * @return list in search order
     * @throws dk.dbc.rawrepo.RawRepoException
     */
    public List<Integer> getAgenciesFor(int agencyId) throws RawRepoException {
        try {
            return cache.get(agencyId);
        } catch (CacheTimeoutException | CacheValueException ex) {
            throw new RawRepoException("Cannot get data: " + ex.getMessage());
        }
    }

    @Override
    public abstract List<Integer> provide(Integer key) throws Exception;
}
