use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("PostgreSQL");
use("DateUtil");

var db = PostgreSQL(System.arguments[0]);
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : "0";
var tracking_base = System.arguments.length > 2 ? System.arguments[2] : ("bulk-" + DateUtil.jsToYYYYmmddHHMMSS(DateUtil.now()) + "-");

function begin() {
    Log.info("System.arguments = " + System.arguments);
    Log.info("parent_agency = " + parent_agencyid);
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
    var id = "(" + bibliographicrecordid + ":" + agencyid + ")";

    Log.debug("bibliographicrecordid=" + bibliographicrecordid);
    Log.debug("agencyid=" + agencyid);
    Log.debug("date=" + date);

    if(!date) {
        var today = new Date();
        date = today.getFullYear() + ("0" + (1 + today.getMonth())).substr(-2) + ("0" + today.getDate()).substr(-2);
    }

    var y = date.substr(0, 4);
    var m = date.substr(4, 2);
    var d = date.substr(6, 2);

    var blob = '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString();
    //var blob = new Binary('<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString(), Binary.Utf8);
    
    Log.info(id + " date=" + date);

    db.begin();

    var sibling = false;
    if (parent_agencyid !== '0' && agencyid !== parent_agencyid) {
	var q = db.prepare("SELECT COUNT(*)::integer AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
	q['bibliographicrecordid'] = bibliographicrecordid;
	q['agencyid'] = parent_agencyid;
	q.execute();
	var r = q.fetch(); 
	var c = r['count'];
	if(c === 1)
	    sibling = true;
    }
    Log.info(id + " is sibling");

    Log.info(id + " update if exists");
    var q = db.prepare("UPDATE records SET CONTENT=encode(:blob, 'BASE64'), mimetype=:mimetype, deleted=FALSE, created=TIMESTAMP WITH TIME ZONE :created, modified=TIMEOFDAY()::TIMESTAMP, trackingid=:trackingid WHERE bibliographicrecordid=:bibliographicrecordid AND agencyid=:agencyid");
    q['blob'] = blob;
    q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange";
    q['created'] = y + "-" + m + "-" + d;
    q['trackingid'] = tracking_base + bibliographicrecordid;
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    if (q.execute() === 0) {
        Log.info(id + " create");
        q.done();
        q = db.prepare("INSERT INTO records(bibliographicrecordid, agencyid, content, mimetype, deleted, created, modified, trackingid) VALUES(:bibliographicrecordid, :agencyid, encode(:blob, 'BASE64'), :mimetype, FALSE, TIMESTAMP WITH TIME ZONE :created, TIMEOFDAY()::TIMESTAMP, :trackingid)");
        q['bibliographicrecordid'] = bibliographicrecordid;
        q['agencyid'] = agencyid;
        q['blob'] = blob;
	q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange";
        q['created'] = y + "-" + m + "-" + d;
        q['trackingid'] = tracking_base + bibliographicrecordid;
        q.execute();
    }
    q.done();

    db.commit();
    print(sibling ? '+' : '·');
}

function error(r) {
    print(r);
}
