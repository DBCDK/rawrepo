/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-content-service
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import javax.annotation.PostConstruct;
import javax.ejb.EJBException;
import javax.ejb.Stateless;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
@Stateless
public class MarcXMergerEJB {

    private static final Logger log = LoggerFactory.getLogger(MarcXMergerEJB.class);
    private MarcXMerger marcXMerger;

    @PostConstruct
    public void init() {
        log.warn("init()");
        try {
            marcXMerger = new MarcXMerger();
        } catch (MarcXMergerException ex) {
            throw new EJBException("Cannot init MarcXChangeMerger", ex);
        }
    }

    public MarcXMerger getMarcXMerger() {
        return marcXMerger;
    }

}
