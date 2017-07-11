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

/* global XmlUtil, XmlNamespaces, Log, solrField, NodeTypes */

use("SolrFields");
use("XmlUtil");
use("XmlNamespaces");
use("NodeTypes");
use("Log");

function add(obj, field) {
    if (obj[field] === undefined)
        obj[field] = [];
    for (var i = 2; i < arguments.length; i++)
        obj[field].push(arguments[i]);
}

var RULES = {
    'danMARC2': {
    }
};

var setup_danmarc_field = function (field, subfield) {
    var dm = RULES['danMARC2'];
    for (var i = 0; i < arguments.length; i++) {
        var spec = arguments[i];
        var m = spec.match(/^(...)(.)$/);
        if (m !== null) {
            var field = dm[m[1]];
            if (field === undefined)
                field = dm[m[1]] = {};
            field[m[2]] = 'marc.' + spec;
        }
    }
};

setup_danmarc_field(
        '004r', '666f', '666e', '666o', '666u');

var dbc_index = function (content, mimetype) {

    var dom = XmlUtil.fromString(content);
    var e = dom.documentElement;

    // Validate (marcx v1 / record)
    if (e.namespaceURI !== XmlNamespaces.marcx.uri || e.localName !== 'record') {
        throw Error("Document not of marcx:record type");
    }

    // find record format
    var format = e.hasAttribute('format') ? e.getAttribute('format') : "danMARC2";

    Log.trace("format = " + format);
    var actions = RULES[format];
    if (actions === undefined) {
        throw Error("Cannot handle record-format: " + format);
    }

    // DEFAULT VALUES
    var obj = {};

    for (var node = e.firstChild; node !== null; node = node.nextSibling) {
        if (node.nodeType === NodeTypes.ELEMENT_NODE && node.namespaceURI === XmlNamespaces.marcx.uri) {
            // marcx v1 / datafield
            if (node.localName === 'datafield') {
                var tag = node.getAttribute('tag');
                var fieldActions = actions[tag]; // action for this tag
                if (fieldActions === undefined) {
                    continue;
                } else if (typeof (fieldActions) === 'function') {
                    fieldActions(obj);
                } else if (typeof (fieldActions) === 'object') {
                    for (var subnode = node.firstChild; subnode !== null; subnode = subnode.nextSibling) {
                        if (subnode.nodeType === NodeTypes.ELEMENT_NODE && subnode.namespaceURI === XmlNamespaces.marcx.uri) {
                            // marcx v1 / subfield
                            if (subnode.localName === 'subfield') {
                                var code = subnode.getAttribute('code');
                                var action = fieldActions[code]; // action for this code
                                if (action === undefined) {
                                    continue;
                                } else if (typeof (action) === 'function') {
                                    Log.trace("Calling function on " + tag + code);
                                    action(obj, XmlUtil.getText(subnode));
                                } else if (typeof (action) === 'string') {
                                    Log.trace("Adding " + tag + code + " to " + action);
                                    if (obj[action] === undefined)
                                        obj[action] = [];
                                    obj[action].push(XmlUtil.getText(subnode));
                                } else {
                                    Log.warn("datafield: " + tag + code + " format: " + format + " invalid data in RULES: type: " + typeof (fieldActions) + " expected function or string");
                                }
                            }
                        }
                    }
                } else {
                    Log.warn("datafield: " + tag + " format: " + format + " invalid data in RULES: type: " + typeof (fieldActions) + " expected function or object");
                }
            }
        }
    }

    for (var i in obj) {
        var a = obj[i];
        if (!(a instanceof Array) || i.indexOf('.') === -1)
            continue;
        for (var n = 0; n < a.length; n++) {
            solrField(i, a[n]);
        }
    }
};
