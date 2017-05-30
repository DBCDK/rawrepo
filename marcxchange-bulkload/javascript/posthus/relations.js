use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("PostgreSQL");
use("Log");

var db = PostgreSQL(System.arguments[0]);
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : '0';

function begin() {
    for (var i = 0; i < System.arguments.length; i++)
        Log.info("System.arguments[" + i + "] = " + System.arguments[i]);
    Log.info("parent_agency = " + parent_agencyid);
}

function end() {
    print("\n");
}

function work(r) {
    Log.trace('row=' + __row__);
    Log.trace(r);
    var current_parent_agencyid = parent_agencyid;
    var xml = XmlUtil.fromString(r);

    var bibliographicrecordid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="a"]', xml);
    var agencyid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="b"]', xml);
    var parent = XPath.selectText('/marcx:record/marcx:datafield[@tag="014"]/marcx:subfield[@code="a"]', xml);
    if (agencyid === "870971") {
        if (XPath.selectText('/marcx:record/marcx:datafield[@tag="014"]/marcx:subfield[@code="x"]', xml) === "ANM") {
            current_parent_agencyid = "870970";
        } else {
            parent = "";
        }
    }
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
        if (current_parent_agencyid !== '0' && agencyid !== current_parent_agencyid) {
            Log.debug(id + " might have sibling");
            try {
                var q = db.prepare("SELECT COUNT(*)::integer AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = current_parent_agencyid;
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
                q['refer_agencyid'] = current_parent_agencyid;
                q.execute();
                q.done();
                s = '«';
                Log.info("Created relation for ", id, " as sibling of ", bibliographicrecordid, ":", current_parent_agencyid)
            } catch (e) {
                Log.error("ERROR: Sibling for " + bibliographicrecordid + " from " + agencyid + " to " + current_parent_agencyid);
                Log.warn(e);
                throw e;
            }
        } else if (parent !== "") {
            var foreign = current_parent_agencyid === '0' ? [agencyid] : [current_parent_agencyid, agencyid];
            var refer_agencyid = null;
            q = db.prepare("SELECT COUNT(*)::integer AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid");
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
            if (refer_agencyid === null) {
                Log.error("Parent (" + parent + ") for " + id + " cannot be found!");
                throw new Error("Parent (" + parent + ") for " + id + " cannot be found!");
            }

            try {
                Log.info(id + " has parent");
                q = db.prepare("INSERT INTO relations(bibliographicrecordid, agencyid, refer_bibliographicrecordid, refer_agencyid) VALUES(:bibliographicrecordid, :agencyid, :refer_bibliographicrecordid, :refer_agencyid)");
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = agencyid;
                q['refer_bibliographicrecordid'] = parent;
                q['refer_agencyid'] = refer_agencyid;
                q.execute();
                q.done();
                s = "^";
                Log.info("Created relation from ", id, " to parent ", parent, ":" + refer_agencyid)
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
