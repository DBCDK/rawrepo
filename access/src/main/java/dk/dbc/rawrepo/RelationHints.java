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
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class RelationHints implements CacheProvider<Integer, List<Integer>> {

    private final GracefulCache<Integer, List<Integer>> cache;

    @SuppressWarnings("LeakingThisInConstructor")
    public RelationHints() {
        this.cache = new GracefulCache<>(this, null, 3600 * 1000, 60 * 1000, 10, 10 * 1000);
    }

    @SuppressWarnings("LeakingThisInConstructor")
    public RelationHints(ExecutorService es) {
        this.cache = new GracefulCache<>(this, es, 3600 * 1000, 60 * 1000, 10, 10 * 1000);
    }

    @Override
    public List<Integer> provide(Integer agencyId) throws Exception {
        throw new IllegalStateException("Relation Hints has not been configured");
    }

    public List<Integer> get(int agencyId) throws CacheTimeoutException, CacheValueException {
        return cache.get(agencyId);
    }

    public boolean usesCommonAgency(int agencyId) throws RawRepoException {
        throw new IllegalStateException("Relation Hints has not been configured");
    }

}
