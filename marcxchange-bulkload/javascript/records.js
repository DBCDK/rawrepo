use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("Binary");
use("PostgreSQL");

var marcx = new Namespace("marcx", "info:lc/xmlns/marcxchange-v1");
var db = PostgreSQL(System.arguments[0]);
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : "191919";

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
    var date = XPath.selectText('/marcx:record/marcx:datafield[@tag="n55"]/marcx:subfield[@code="a"]', xml);
    XPath.delete('/marcx:record/marcx:datafield[@tag="n55"]', xml);

    Log.debug("bibliographicrecordid=" + bibliographicrecordid);
    Log.debug("agencyid=" + agencyid);
    Log.debug("date=" + date);

    if(!date) {
        var today = new Date();
        date = ("0" + today.getDate()).substr(-2) + ("0" + (1 + today.getMonth())).substr(-2) + today.getFullYear();
    }

    var y = date.substr(0, 4);
    var m = date.substr(4, 2);
    var d = date.substr(6, 2);

    var blob = '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString();

    db.begin();

    var sibling = false;
    if (agencyid !== parent_agencyid) {
	var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
	q['bibliographicrecordid'] = bibliographicrecordid;
	q['agencyid'] = parent_agencyid;
	q.execute();
	var r = q.fetch(); 
	var c = r['count'];
	if(c === 1)
	    sibling = true;
    }


    var q = db.prepare("UPDATE records SET CONTENT=encode(:blob, 'BASE64'), mimetype=:mimetype, created=:created, modified=TIMEOFDAY()::TIMESTAMP WHERE bibliographicrecordid=:id AND agencyid=:agencyid");
    q['blob'] = blob;
    q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange";
    q['created'] = y + "-" + m + "-" + d;
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    if (q.execute() === 0) {
        q.done();
        q = db.prepare("INSERT INTO records(bibliographicrecordid, agencyid, content, mimetype, created, modified) VALUES(:id, :agencyid, encode(:blob, 'BASE64'), :mimetype, :created, TIMEOFDAY()::TIMESTAMP)");
        q['bibliographicrecordid'] = bibliographicrecordid;
        q['agencyid'] = agencyid;
        q['blob'] = blob;
	q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange";
        q['created'] = y + "-" + m + "-" + d;
        q.execute();
    }
    q.done();

    db.commit();
    print(sibling ? '+' : '·');
}

