use("Print");
use("Log");
use("XmlUtil");
use("Binary");
use("PostgreSQL");

var marcx = new Namespace("marcx", "info:lc/xmlns/marcxchange-v1");
var db = PostgreSQL(System.arguments[0]);

function begin() {
}

function end() {
    print("\n");
}

function work(r) {
    Log.debug(r);
    var xml = XmlUtil.fromString(r);

    var id   = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "a"));
    var lib  = String(xml.marcx::datafield.(@tag == "001").marcx::subfield.(@code == "b"));
    var date = String(xml.marcx::datafield.(@tag == "n55").marcx::subfield.(@code == "a"));
    Log.debug("id=" + id);
    Log.debug("lib=" + lib);
    Log.debug("date=" + date);

    var y = date.substr(0, 4);
    var m = date.substr(4, 2);
    var d = date.substr(6, 2);

    var blob = '<?xml version="1.0" encoding="UTF-8"?>' + "\n" + xml.toString();

    db.begin();

    var q = db.prepare("UPDATE records SET CONTENT=encode(:blob, 'BASE64'), created=:created, modified=TIMEOFDAY()::TIMESTAMP WHERE id=:id AND library=:lib");
    q['blob'] = blob;
    q['created'] = y + "-" + m + "-" + d;
    q['id'] = id;
    q['lib'] = lib;
    if (q.execute() === 0) {
        q.done();
        q = db.prepare("INSERT INTO records(id, library, content, created, modified) VALUES(:id, :lib, encode(:blob, 'BASE64'), :created, TIMEOFDAY()::TIMESTAMP)");
        q['id'] = id;
        q['lib'] = lib;
        q['blob'] = blob;
        q['created'] = y + "-" + m + "-" + d;
        q.execute();
    }
    q.done();

    db.commit();
    print('*');
}

