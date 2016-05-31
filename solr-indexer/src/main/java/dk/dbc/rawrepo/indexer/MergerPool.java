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

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.inject.Singleton;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class MergerPool {
    Set<MarcXMerger> mergers = new HashSet<>();


    public MarcXMerger getMerger() throws MarcXMergerException {
        synchronized(this) {
            if(mergers.isEmpty())
                return new MarcXMerger();
            Iterator<MarcXMerger> iterator = mergers.iterator();
            MarcXMerger merger = iterator.next();
            iterator.remove();
            return merger;
        }
    }

    public void putMerger(MarcXMerger merger) {
        synchronized(this) {
            mergers.add(merger);
        }
    }

}
