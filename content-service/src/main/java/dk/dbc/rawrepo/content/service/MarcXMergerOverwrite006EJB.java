/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.EJBException;
import javax.inject.Singleton;

/**
 * This version of MarcXMerger returns a merge which overwrites field 006 with content from the enrichment
 *
 * @author DBC {@literal <dbc.dk>}
 */
@Singleton
public class MarcXMergerOverwrite006EJB extends Pool<MarcXMerger> {

    private static final Logger log = LoggerFactory.getLogger(MarcXMergerOverwrite006EJB.class);

    @PostConstruct
    public void init() {
        log.warn("init()");
    }

    @Override
    public MarcXMerger create() {
        try {
            final String overwrite = "001;004;005;006;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

            FieldRules customFieldRules = new FieldRules(FieldRules.IMMUTABLE_DEFAULT, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);

            return new MarcXMerger(customFieldRules);
        } catch (MarcXMergerException ex) {
            throw new EJBException("Cannot init MarcXChangeMerger", ex);
        }
    }

}
