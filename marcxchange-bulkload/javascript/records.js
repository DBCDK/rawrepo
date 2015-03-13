use("Print");
use("Log");
use("XmlUtil");
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
    Log.debug(r);
    var xml = XmlUtil.fromString(r);

    var id   = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "a"));
    var agencyid  = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "b"));
    var date = String(xml.marcx::datafield.(@tag == "n55").marcx::subfield.(@code == "a"));
    delete xml.marcx::datafield.(@tag == "n55")[0];

    Log.debug("id=" + id);
    Log.debug("agencyid=" + agencyid);
    Log.debug("date=" + date);

    var y = date.substr(0, 4);
    var m = date.substr(4, 2);
    var d = date.substr(6, 2);

    var blob = '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString();

    db.begin();

    var sibling = false;
    if (agencyid !== parent_agencyid) {
	var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
	q['bibliographicrecordid'] = id;
	q['agencyid'] = parent_agencyid;
	q.execute();
	var r = q.fetch(); 
	var c = r['count'];
	if(c === 1)
	    sibling = true;
    }


    var q = db.prepare("UPDATE records SET CONTENT=encode(:blob, 'BASE64'), mimetype=:mimetype, created=:created, modified=TIMEOFDAY()::TIMESTAMP WHERE bibliographicrecordid=:id AND agencyid=:agencyid");
    q['blob'] = blob;
    q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange"
    q['created'] = y + "-" + m + "-" + d;
    q['bibliographicrecordid'] = id;
    q['agencyid'] = agencyid;
    if (q.execute() === 0) {
        q.done();
        q = db.prepare("INSERT INTO records(bibliographicrecordid, agencyid, content, mimetype, created, modified) VALUES(:id, :agencyid, encode(:blob, 'BASE64'), :mimetype, :created, TIMEOFDAY()::TIMESTAMP)");
        q['bibliographicrecordid'] = id;
        q['agencyid'] = agencyid;
        q['blob'] = blob;
	q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange"
        q['created'] = y + "-" + m + "-" + d;
        q.execute();
    }
    q.done();

    db.commit();
    print(sibling ? '+' : '·');
}

