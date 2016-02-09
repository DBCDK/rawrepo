/* global XmlUtil, XmlNamespaces, Log, solrField */

use("SolrFields");
use("XmlUtil");
use("XmlNamespaces");
use("Log");

var COLLECTION_IDENTIFIER = 'rec.collectionIdentifier';

function add(obj, field) {
    if (obj[field] === undefined)
        obj[field] = [];
    for (var i = 2; i < arguments.length; i++)
        obj[field].push(arguments[i]);
}

function addSolrTime(obj, field, value) {
    if (value.length === 8) {
        add(obj, field,
                value.slice(0, 4) + "-" +
                value.slice(4, 6) + "-" +
                value.slice(6, 8) + "T00:00:00Z");
    } else if (value.length === 14) {
        add(obj, field,
                value.slice(0, 4) + "-" +
                value.slice(4, 6) + "-" +
                value.slice(6, 8) + "T" +
                value.slice(8, 10) + ":" +
                value.slice(10, 12) + ":" +
                value.slice(12, 14) + "Z");
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
                if (val === '191919')
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
            if (obj['agency'] === '870970')
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
        '002a', '008a', '009a', '009g', '014a', '021a', '021e', '022a',
        '023a', '023b', '024a', '028a', '100a', '110a', '245a', '245ø',
        '250a', '260b', '300e', '538g', '652m', 'y08a'
        );

var index = function (content, mimetype) {

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
    obj[COLLECTION_IDENTIFIER] = ['any'];

    for (var node = e.firstChild; node !== null; node = node.nextSibling) {
        if (node.nodeType === node.ELEMENT_NODE && node.namespaceURI === XmlNamespaces.marcx.uri) {
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
                        if (subnode.nodeType === subnode.ELEMENT_NODE && subnode.namespaceURI === XmlNamespaces.marcx.uri) {
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
}
