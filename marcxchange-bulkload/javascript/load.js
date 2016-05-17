use("Print");
use("Log");
use("XmlUtil");
use("Binary");
use("PostgreSQL");
use("Log");


var marcx = new Namespace( "marcx", "info:lc/xmlns/marcxchange-v1" );
var db = PostgreSQL(System.arguments[0]);
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : "870970";

function begin() {
}

function end() {
    print("\n");
}

function work(r) {
    var xml = XmlUtil.fromString(r);

    var id       = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "a"));
    var agencyid = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "b"));
    var parent   = String(xml.marcx::datafield.(@tag == "014").marcx::subfield.(@code == "a"));
    var date     = String(xml.marcx::datafield.(@tag == "n55").marcx::subfield.(@code == "a"));
    delete xml.marcx::datafield.(@tag == "n55")[0];

    Log.debug("id=" + id);
    Log.debug("agencyid=" + agencyid);
    Log.debug("parent=" + parent);
    Log.debug("date=" + date);

    var y = date.substr(0, 4);
    var m = date.substr(4, 2);
    var d = date.substr(6, 2);
    var created = y + "-" + m + "-" + d;
    var blob = '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString();

    var recid = id + "/" + agencyid

    db.begin();
    try {
	var s = "·"

	// Has Sibling ?
	var sibling = false;
	if (agencyid !== parent_agencyid) {
	    try {
		var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
		q['bibliographicrecordid'] = id;
		q['agencyid'] = parent_agencyid;
		q.execute();
		var r = q.fetch(); 
		var c = r['count'];
		if(c === 1) {
		    Log.debug("Has sibling");
		    sibling = true;
		}
            } catch (e) {
		Log.error("ERROR: Find sibling for: " + recid);
		Log.warn(e);
		throw e;
	    }
	}
	
	// Update / Create record
	try {
	    var q = db.prepare("UPDATE records SET CONTENT=encode(:blob, 'BASE64'), mimetype=:mimetype, deleted=FALSE, created=:created, modified=TIMEOFDAY()::TIMESTAMP WHERE bibliographicrecordid=:id AND agencyid=:agencyid");
	    q['blob'] = blob;
	    q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange"
	    q['created'] = y + "-" + m + "-" + d;
	    q['bibliographicrecordid'] = id;
	    q['agencyid'] = agencyid;
	    if (q.execute() === 0) {
		q.done();
		q = db.prepare("INSERT INTO records(bibliographicrecordid, agencyid, content, mimetype, deleted, created, modified) VALUES(:id, :agencyid, encode(:blob, 'BASE64'), :mimetype, FALSE, :created, TIMEOFDAY()::TIMESTAMP)");
		q['bibliographicrecordid'] = id;
		q['agencyid'] = agencyid;
		q['blob'] = blob;
		q['mimetype'] = sibling ? "text/enrichment+marcxchange" : "text/marcxchange"
		q['created'] = y + "-" + m + "-" + d;
		q.execute();
	    }
	    q.done();
        } catch (e) {
	    Log.error("ERROR: UPDATE/CREATE for: " + recid);
	    Log.warn(e);
	    throw e;
	}

	// Remove relations
	try {
	    var q = db.prepare("DELETE FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
	    q['bibliographicrecordid'] = id;
	    q['agencyid'] = agencyid;
	    q.execute();
	    q.done();
        } catch (e) {
	    Log.error("ERROR: DELETE relations for: " + recid);
            Log.warn(e);
	    throw e;
	}

	if (sibling) {
	    // Sibling relation
	    try {
		q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:bibliographicrecordid, :agencyid, :refer_bibliographicrecordid, :ref_agencyid)");
		q['bibliographicrecordid'] = id;
		q['agencyid'] = agencyid;
		q['refer_bibliographicrecordid'] = id;
		q['refer_agencyid'] = parent_agencyid;
		q.execute();
		q.done();
		s = '+';
	    } catch (e) {
		Log.error("ERROR: CREATE Sibling relation for " + recid + " to " + parent_agencyid);
		Log.warn(e);
		throw e;
	    }
	} else if (parent !== "") {
	    // Parent relation
	    try {
		q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:id, :agencyid, :ref_id, :ref_agencyid)");
		q['bibliographicrecordid'] = id;
		q['agencyid'] = agencyid;
		q['refer_bibliographicrecordid'] = parent;
		q['refer_agencyid'] = agencyid;
		q.execute();
		q.done();
		s = "^";
	    } catch (e) {
		Log.error("ERROR: CREATE Parent relation for " + recid + " to " + parent);
		Log.warn(e);
		throw e;
            }
	}
	
	print(s);
	db.commit();
    } catch (e) {
	print("*");
        db.rollback();
	db.begin();
    }
}
