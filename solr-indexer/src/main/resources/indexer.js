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
use("MarcXchange");

var COLLECTION_IDENTIFIER = 'rec.collectionIdentifier';

function add(obj, field) {
    if (obj[field] === undefined)
        obj[field] = [];
    for (var i = 2; i < arguments.length; i++)
        obj[field].push(arguments[i]);
}

function pad(number) {
    if (number < 10) {
        return '0' + number;
    }
    return number;
}

function addSolrTime(obj, field, value) {
//    Log.error("value = " + value);
    var iso8601;
    if (value.length === 8) {
        iso8601 = value.slice(0, 4) + "-" +
                value.slice(4, 6) + "-" +
                value.slice(6, 8) + "T00:00:00Z";
    } else if (value.length === 14) {
        iso8601 = value.slice(0, 4) + "-" +
                value.slice(4, 6) + "-" +
                value.slice(6, 8) + "T" +
                value.slice(8, 10) + ":" +
                value.slice(10, 12) + ":" +
                value.slice(12, 14) + "Z";
    } else {
        return;
    }
//    Log.trace("iso8601 = " + iso8601);
    var ts = new Date(iso8601);
    if (!isNaN(ts.getTime())) {
        var iso8601_parsed = ts.getUTCFullYear() + '-' +
                pad(ts.getUTCMonth() + 1) + '-' +
                pad(ts.getUTCDate()) + 'T' +
                pad(ts.getUTCHours()) + ':' +
                pad(ts.getUTCMinutes()) + ':' +
                pad(ts.getUTCSeconds()) + "Z";
        if (iso8601_parsed === iso8601)
            add(obj, field, iso8601);
    }
}

var RULES = {
    'danMARC2': {
        '001': {
            'a': function (obj, val) {
                obj['record'] = val;
                add(obj, "marc.001a", val);
                if ('agency' in obj) {
                    add(obj, "marc.001a001b", obj['record'] + ':' + obj['agency']);
                }
            },
            'b': function (obj, val) {
                Log.trace("001b" + val);
                obj['agency'] = val;
                if (val === '870970')
                    obj[COLLECTION_IDENTIFIER] = ['common'];
                add(obj, 'marc.001b', val);
                if ('record' in obj) {
                    add(obj, "marc.001a001b", obj['record'] + ':' + obj['agency']);
                }
            },
            'c': function (obj, val) {
                addSolrTime(obj, 'marc.001c', val);
            },
            'd': function (obj, val) {
                addSolrTime(obj, 'marc.001d', val);
            }
        },
        's11': function (obj) {
            if (obj['agency'] === '191919')
                obj[COLLECTION_IDENTIFIER] = ['dbc'];
        }
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
    '002a', '002b', '002c', '002x',
    '004a',
    '008a',
    '009a', '009g',
    '014a',
    '021a', '021e',
    '022a',
    '023a', '023b',
    '024a',
    '028a',
    '100a',
    '110a',
    '245a', '245g', '245n', '245ø',
    '250a',
    '260b',
    '300e',
    '538g',
    '652m',
    'y08a',
    's11a');

var createMarc21Index = function ( xmlRecord ) {
    var marcRecord = MarcXchange.marcXchangeToMarcRecord ( xmlRecord );
    var indexObject = {};
    var field001 = marcRecord.field("001").value;
    var field003 = marcRecord.field("003").value;
    var field245a = marcRecord.field("245").subfield("a").value;
    indexObject["marc21.001"] = [ field001 ];
    indexObject["marc21.003"] = [ field003 ];
    indexObject["marc21.245a"] = [ field245a ];
    return indexObject
};

var index = function (content, mimetype) {

    var dom = XmlUtil.fromString(content);

    var indexObject = createMarc21Index( dom );
    for (var i in indexObject) {
        var a = indexObject[i];
        if (!(a instanceof Array) || i.indexOf('.') === -1)
            continue;
        for (var n = 0; n < a.length; n++) {
            solrField(i, a[n]);
        }
    }
};
