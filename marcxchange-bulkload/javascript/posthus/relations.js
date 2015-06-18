use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("Binary");
use("PostgreSQL");
use("Log");


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
    var parent = XPath.selectText('/marcx:record/marcx:datafield[@tag="014"]/marcx:subfield[@code="a"]', xml);

    Log.debug("bibliographicrecordid=" + bibliographicrecordid);
    Log.debug("agencyid=" + agencyid);
    Log.debug("parent=" + parent);

    db.begin();
    try {
        try {
            var q = db.prepare("DELETE FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
            q['bibliographicrecordid'] = bibliographicrecordid;
            q['agencyid'] = agencyid;
            q.execute();
            q.done();
        } catch (e) {
            Log.error("Delete relations for: " + bibliographicrecordid);
            Log.error(e);
            throw e;
        }

        var s = "·"
        var sibling = false;
        if (agencyid !== parent_agencyid) {
            Log.debug("Might have sibling");
            try {
                var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = parent_agencyid;
                q.execute();
                var r = q.fetch();
                var c = r['count'];
                if (c === 1) {
                    Log.debug("Has sibling");
                    sibling = true;
                }
            } catch (e) {
                Log.error("Find sibling for: " + bibliographicrecordid);
                Log.error(e);
                throw e;
            }
        }

        if (sibling) {
            try {
                q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:bibliographicrecordid, :agencyid, :refer_bibliographicrecordid, :ref_agencyid)");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = agencyid;
                q['refer_bibliographicrecordid'] = bibliographicrecordid;
                q['refer_agencyid'] = parent_agencyid;
                q.execute();
                q.done();
                s = '«';
            } catch (e) {
                Log.error("ERROR: Sibling for " + bibliographicrecordid + " from " + agencyid + " to " + parent_agencyid);
                Log.warn(e);
                throw e;
            }
        } else if (parent !== "") {
            var foreign = [agencyid, parent_agencyid];
            var refer_agencyid = null;
            q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
            for (var i = 0; i < foreign.length && refer_agencyid === null; i++) {
                Log.warn("i=" + i);
                q['bibliographicrecordid'] = parent;
                q['agencyid'] = foreign[i];
                q.execute();
                var r = q.fetch();
                var c = r['count'];
                if (c === 1) {
                    refer_agencyid = foreign[i];
                }
            }
            q.done();
            if(refer_agencyid === null) {
                Log.error("Parent (" + parent + ") for " + bibliographicrecordid + " from " + agencyid + " cannot be found!");
                throw new Error("Parent (" + parent + ") for " + bibliographicrecordid + " from " + agencyid + " cannot be found!");
            }

            try {
                q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:id, :agencyid, :ref_id, :ref_agencyid)");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = agencyid;
                q['refer_bibliographicrecordid'] = parent;
                q['refer_agencyid'] = refer_agencyid;
                q.execute();
                q.done();
                s = "^";
            } catch (e) {
                Log.error("Parent " + parent + " for " + bibliographicrecordid + " from " + agencyid);
                Log.warn(e);
                throw e;
            }
        }
        print(s);

        db.commit();
    } catch (e) {
        print("*");
        db.rollback();
        throw e;
    }
}
