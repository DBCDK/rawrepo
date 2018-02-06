import psycopg2
import sys


conn = None
try:
    conn = psycopg2.connect("dbname=db_database")
    cur = conn.cursor()

    counter = 0
    cur.callproc('dequeue', ('performance_test', 1))
    row = cur.fetchone()
    while row is not None:
        conn.commit()
        counter = counter + 1
        if counter % 1000 == 0:
            print "rows %d" % counter
            sys.stdout.flush()
        if counter == int(sys.argv[1]):
            break
        cur.callproc('dequeue', ('performance_test', 1))
        row = cur.fetchone()
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
