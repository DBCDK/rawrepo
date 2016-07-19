use("Print");
use("Log");
use("XmlUtil");
use("XPath");
use("XmlNamespaces");
use("Binary");
use("PostgreSQL");

var marcx = new Namespace("marcx", "info:lc/xmlns/marcxchange-v1");
var db = PostgreSQL(System.arguments[0]);
var parent_agencyid = System.arguments.length > 1 ? System.arguments[1] : '';
var provider = System.arguments.length > 2 ? System.arguments[2] : "opencataloging-update";

function begin() {
    Log.info("System.arguments = " + System.arguments);
    Log.info("parent_agencyid = " + parent_agencyid);
    Log.info("provider = " + provider);
}

function end() {
    print("\n");
}

function findMostCommonAgency(bibliographicrecordid, agencyidList) {
    var q = db.prepare("SELECT COUNT(*) AS count FROM records WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid AND deleted = false");
    for (var i = 0; i < agencyidList.length; i++) {
        var agencyid = agencyidList[i];
        q['bibliographicrecordid'] = bibliographicrecordid;
        q['agencyid'] = agencyid;
        q.execute();
        var r = q.fetch();
        var exists = r['count'] !== 0;

        if (exists) {
            q.done();
            q = db.prepare("SELECT refer_agencyid FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid AND refer_bibliographicrecordid = bibliographicrecordid");
            while (true) {
                q['bibliographicrecordid'] = bibliographicrecordid;
                q['agencyid'] = agencyid;
                q.execute();
                r = q.fetch();
                if (r === undefined)
                    break;
                agencyid = r['refer_agencyid'];
            }
            q.done();
            return agencyid;
        }

    }
    q.done();
    return undefined;
}

function findLeaves(bibliographicrecordid, agencyidList) {
    var agencyid = findMostCommonAgency(bibliographicrecordid, agencyidList);
    if (agencyid === undefined)
        return [];
    var leaves = [];
    var q = db.prepare("SELECT bibliographicrecordid FROM relations WHERE refer_bibliographicrecordid = :bibliographicrecordid AND refer_agencyid = :agencyid AND refer_bibliographicrecordid <> bibliographicrecordid");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    q.execute();
    for (var r = q.fetch(); r !== undefined; r = q.fetch()) {
        var child = r['bibliographicrecordid'];
        var r = findLeaves(child, agencyidList);
        if (r.length === 0)
            r = [child];
        leaves = leaves.concat(r);
    }
    q.done();
    return leaves;
}

function work(r) {
    Log.trace(r);
    var xml = XmlUtil.fromString(r);

    var bibliographicrecordid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="a"]', xml);
    var agencyid = XPath.selectText('/marcx:record/marcx:datafield[@tag="001"]/marcx:subfield[@code="b"]', xml);
    var id = "(" + bibliographicrecordid + ":" + agencyid + ")";

    Log.info(id)
    db.begin();

    // Inbound relations to this id (any agency)
    var q = db.prepare("SELECT COUNT(*) AS count FROM relations WHERE refer_bibliographicrecordid <> bibliographicrecordid AND refer_bibliographicrecordid = :bibliographicrecordid AND refer_agencyid IN (:common, :agencyid)");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['common'] = parent_agencyid === '' ? agencyid : parent_agencyid;
    q['agencyid'] = agencyid;
    q.execute();
    var r = q.fetch();
    var leaf = r['count'] === 0;
    q.done();

    // Outbound sibling relation
    var q = db.prepare("SELECT COUNT(*) AS count FROM relations WHERE bibliographicrecordid = :bibliographicrecordid AND agencyid = :agencyid AND refer_bibliographicrecordid = bibliographicrecordid");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    q.execute();
    var r = q.fetch();
    var standAlone = r['count'] === 0;
    q.done();

    q = db.prepare("SELECT * FROM enqueue(:bibliographicrecordid, CAST(:agencyid AS int), :mimetype, :provider, :changed, :leaf)");
    q['bibliographicrecordid'] = bibliographicrecordid;
    q['agencyid'] = agencyid;
    q['mimetype'] = standAlone ? "text/marcxchange" : "text/enrichment+marcxchange";
    q['provider'] = provider;
    q['changed'] = "Y";
    q['leaf'] = leaf ? "Y" : "N";
    q.execute();
    Log.info(id + " queued " + bibliographicrecordid + " with changed=Y & leaf=" + (leaf ? "Y" : "N"));
    if (!leaf) {
        var leaves = findLeaves(bibliographicrecordid, parent_agencyid === '' ? [agencyid] : [agencyid, parent_agencyid]);
        for (var i = 0; i < leaves.length; i++) {
            q['bibliographicrecordid'] = leaves[i];
            q['agencyid'] = agencyid;
            q['mimetype'] = standAlone ? "text/marcxchange" : "text/enrichment+marcxchange";
            q['provider'] = provider;
            q['changed'] = "N";
            q['leaf'] = "Y";
            q.execute();
            Log.info(id + " queued " + leaves[i] + " with changed=N & leaf=Y");
        }
    }
    q.done();
    db.commit();
    print('·');
}

function error(r) {
    print(r);
}
