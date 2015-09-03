/* global XmlUtil, XmlNamespaces, Log, solrField */

use("SolrFields");
use("XmlUtil");
use("XmlNamespaces");
use("Log");

var COLLECTION_IDENTIFIER = 'rec.collectionIdentifier';

var RULES = {
    'danMARC2': {
        '001': {
            'b': function (obj, val) {
                Log.trace("001b" + val);
                obj['agency'] = val;
                if(val === '191919')
                    obj[COLLECTION_IDENTIFIER] = ['common'];
            }
        },
        '002': {
            'a': 'marc.002a'
        },
        '021': {
            'a': 'marc.021ae',
            'e': 'marc.021ae'
        },
        '022': {
            'a': 'marc.022a'
        },
        's11': function (obj) {
            if (obj['agency'] === '870970')
                obj[COLLECTION_IDENTIFIER] = ['dbc'];
        }
    }
};


function index(content, mimetype) {

    var dom = XmlUtil.fromString(content);
    var e = dom.documentElement;

    // Validate (marcx v1 / record)
    if (e.namespaceURI !== XmlNamespaces.marcx.uri || e.localName !== 'record') {
        throw Error("Document not of marcx:record type");
    }

    // find record format
    var format = e.getAttribute('format');
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
        if (!(a instanceof Array))
            continue;
        for (var n = 0; n < a.length; n++) {
            solrField(i, a[n]);
        }
    }
}
