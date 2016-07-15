/* global Log, XmlUtil, XPath, System, __row__ */

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
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : null;
if(parent_agency === '')
    parent_agency = null

function begin() {
 Log.trace("db = " + System.arguments[0]);
}

function end() {
    print("\n");
}

function work(r) {
    Log.trace('row=' + __row__);
    Log.trace(r);
    var xml = XmlUtil.fromString(r);

    var bibliographicrecordid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="a"]', xml);
    var agencyid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="b"]', xml);
    var parent = XPath.selectText('/marcx:record/marcx:datafield[@tag="014"]/marcx:subfield[@code="a"]', xml);
    var id = "(" + bibliographicrecordid + ":" + agencyid + ")";

    Log.info(id + " parent=" + parent);

    db.begin();
    try {
        try {
            var q = db.prepare("DELETE FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
            q['bibliographicrecordid'] = bibliographicrecordid;
            q['agencyid'] = agencyid;
            q.execute();
            q.done();
        } catch (e) {
            Log.error(id + " delete relations");
            Log.error(e);
            throw e;
        }

        var s = "·";
        var sibling = false;
        if (parent_agencyid !== null && agencyid !== parent_agencyid) {
            Log.debug(id + " might have sibling");
            try {
                var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = parent_agencyid;
                q.execute();
                var r = q.fetch();
                var c = r['count'];
                if (c === 1) {
                    Log.debug(id + " is sibling");
                    sibling = true;
                }
            } catch (e) {
                Log.error(id + " find sibling");
                Log.error(e);
                throw e;
            }
        }

        if (sibling) {
            try {
                Log.info(id + " is sibling");
                q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:bibliographicrecordid, :agencyid, :refer_bibliographicrecordid, :refer_agencyid)");
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
            var foreign = parent_agencyid === null ? [agencyid] : [parent_agencyid, agencyid];
            var refer_agencyid = null;
            q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
            for (var i = 0; i < foreign.length && refer_agencyid === null; i++) {
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
                Log.error("Parent (" + parent + ") for " + id + " cannot be found!");
                throw new Error("Parent (" + parent + ") for " + id + " cannot be found!");
            }

            try {
                Log.info(id + " has parent");
                q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:id, :agencyid, :ref_id, :ref_agencyid)");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = agencyid;
                q['refer_bibliographicrecordid'] = parent;
                q['refer_agencyid'] = refer_agencyid;
                q.execute();
                q.done();
                s = "^";
            } catch (e) {
                Log.error("Parent " + parent + " for " + id);
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

function error(r) {
    print(r);
}
