use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("Binary");
use("PostgreSQL");

var marcx = new Namespace("marcx", "info:lc/xmlns/marcxchange-v1");
var db = PostgreSQL(System.arguments[0]);
var parent_agency = System.arguments.length > 1 ? System.arguments[1] : "191919";
var provider = System.arguments.length > 2 ? System.arguments[2] : "opencataloging-update";

function begin() {
}

function end() {
    print("\n");
}

function work(r) {
    Log.trace(r);
    var xml = XmlUtil.fromString(r);

    var bibliographicrecordid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="a"]', xml);
    var agencyid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="b"]', xml);

    Log.debug("bibliographicrecordid=" + bibliographicrecordid);
    Log.debug("agencyid=" + agencyid);

    db.begin();

    // Inbound relations to this id (any agency)
    var q = db.prepare("SELECT COUNT(*) AS count FROM relations WHERE refer_bibliographicrecordid <> bibliographicrecordid AND refer_bibliographicrecordid = :bibliographicrecordid AND refer_agencyid IN (:common, :agencyid)");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['common'] = parent_agency;
    q['agencyid'] = agencyid;
    var r = q.fetch();
    var leaf = r['count'] === 0 ? "Y" : "N";
    q.done();
    // Outbound sibling relation
    var q = db.prepare("SELECT COUNT(*) AS count FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid AND refer_bibliographicrecordid = bibliographicrecordid");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    var r = q.fetch();
    var mimetype = r['count'] === 0 ? "text/marcxchange" : "text/enrichment+marcxchange";
    q.done();

    q = db.prepare("SELECT * FROM enqueue(:bibliographicrecordid, :agency, :mimetype, :provider, 'Y', :leaf)");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    q['mimetype'] = mimetype;
    q['provider'] = provider;
    q['leaf'] = leaf;
    q.execute();

    db.commit();
    print('·');
}

