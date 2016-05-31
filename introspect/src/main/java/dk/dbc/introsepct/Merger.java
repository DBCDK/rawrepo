/*
 * dbc-rawrepo-introspect
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-introspect.
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-introspect.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.introsepct;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class Merger {

    private static final Logger log = LoggerFactory.getLogger(Merger.class);

    MarcXMerger merger;

    @PostConstruct
    public void postConstruct() {
        log.debug("postConstruct");
        try {
            merger = new MarcXMerger();
        } catch (MarcXMergerException ex) {
            log.error("Cannot make merger", ex);
        }
    }

    public MarcXMerger getMerger() {
        log.debug("getMerger()");
        return merger;
    }

}
