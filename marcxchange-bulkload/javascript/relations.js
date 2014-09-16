use("Print");
use("Log");
use("XmlUtil");
use("Binary");
use("PostgreSQL");
use("Log");


var marcx = new Namespace( "marcx", "info:lc/xmlns/marcxchange-v1" );
var db = PostgreSQL(System.arguments[0]);

function begin() {
}

function end() {
    print("\n");
}

function work(r) {
    Log.debug(r);
    var xml = XmlUtil.fromString(r);

    var id     = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "a"));
    var lib   = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "b"));
    var parent = String(xml.marcx::datafield.(@tag == "014").marcx::subfield.(@code == "a"));
    Log.debug("id=" + id);
    Log.debug("lib=" + lib);
    Log.debug("parent=" + parent);

    if (parent === "") {
        print("-");
    } else {
        db.begin();

        try {
            var q = db.prepare("DELETE FROM relations WHERE id = :id AND library = :lib");
            q['id'] = id;
            q['lib'] = lib;
            q.execute();
            q.done();

            q = db.prepare("INSERT INTO relations(id, library, refer_id, refer_library) VALUES(:id, :lib, :ref_id, :ref_lib)");
            q['id'] = id;
            q['lib'] = lib;
            q['ref_id'] = parent;
            q['ref_lib'] = lib;
            q.execute();
            q.done();
            print("+");
        } catch (e) {
            Log.error(e);
            db.rollback();
            print('*' + id + '*');
        }

        db.commit();
    }
}
