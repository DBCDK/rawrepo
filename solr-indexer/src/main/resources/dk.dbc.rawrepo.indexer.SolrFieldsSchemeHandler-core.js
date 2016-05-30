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

/* global __SolrFields */

/**
 * exports SolrFields with method:
 * SolrFields.addSolrField
 *
 * global variable __SolrFields contains target objecct, that implements void addField(String, String)
 */
EXPORTED_SYMBOLS = ['solrField'];

var solrField = function (javaFunc) {

    return  function (name, value) {
        javaFunc(name, value);
    };

}(Function.prototype.bind.call(__SolrFields.addField, __SolrFields));

delete __SolrFields;
