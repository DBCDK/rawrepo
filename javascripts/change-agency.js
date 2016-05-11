use('Print');
use('System');
use('Log');
use('XmlUtil');
use('XPath');
use('XmlNamespaces');

var agencyId = System.arguments[0];

function begin() {
    Log.info("In begin()");
    printn('<?xml version="1.0" encoding="utf8"?>');
    printn('<marcx:collection xmlns:marcx="info:lc/xmlns/marcxchange-v1">')

}

function end() {
    Log.info("In end()");
    printn('</marcx:collection>')
}

function work(xml) {
    var dom = XmlUtil.fromString(xml);
    var agencyElement = XPath.selectNode('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="b"]/text()', dom);
    agencyElement.nodeValue = agencyId;
    printn(dom);
}

