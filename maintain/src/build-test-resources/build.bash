#!/bin/bash -e


unset PGUSER
unset PGPASSWORD
unset PGHOST
unset PGPORT
unset PGDATABASE


PGUSER=duser
PGPASSWORD=dpass
PGHOST=localhost
PGPORT=5431
PGDATABASE=data
DATADIR=/tmp/$USER.pg

export PGHOST
export PGPORT


V="`ls -f /usr/lib/postgresql/ | sort -n | tail -n 1`"
if [ "$V" == "" ]; then
    echo "Could not find a PostgreSQL installation"
    exit 1
fi

rm -rf $DATADIR

/usr/lib/postgresql/$V/bin/pg_ctl initdb -D $DATADIR/data

echo 'host all all 0.0.0.0/0 trust' >> $DATADIR/data/pg_hba.conf
/usr/lib/postgresql/$V/bin/pg_ctl start \
				  -D $DATADIR/data -l $DATADIR/log \
				  -o "-p $PGPORT -i -k $DATADIR/"
for ok in $(seq 0 50) fail; do
    if psql postgres -c 'SELECT VERSION();' >/dev/null; then
	break;
    fi
    sleep .1
done
if [ $ok = fail ]; then
    echo "Could not start postgresql server"
    exit 1
fi

psql postgres <<EOF
${FAIL+\set ON_ERROR_STOP}
CREATE DATABASE $PGDATABASE;
CREATE ROLE $PGUSER WITH PASSWORD '${PGPASSWORD//\'/''}' LOGIN SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE $PGDATABASE TO $PGUSER;
EOF

/usr/lib/postgresql/$V/bin/pg_ctl stop -m fast -D $DATADIR/data

rm -f $DATADIR/data/pg_hba.conf
echo 'host all all 0.0.0.0/0 md5' >> $DATADIR/data/pg_hba.conf
echo 'host all all ::0/0 md5' >> $DATADIR/data/pg_hba.conf
echo "fsync = off" >> $DATADIR/data/postgresql.conf
echo "full_page_writes = off" >> $DATADIR/data/postgresql.conf

export PGUSER
export PGPASSWORD
export PGDATABASE

/usr/lib/postgresql/$V/bin/pg_ctl start \
				  -D $DATADIR/data -l $DATADIR/log \
				  -o "-p $PGPORT -i -k $DATADIR/"


for ok in $(seq 0 50) fail; do
    if psql -c 'SELECT VERSION();' >/dev/null; then
	break;
    fi
    sleep .1
done
if [ $ok = fail ]; then
    echo "Could not start postgresql server"
    exit 1
fi

trap "/usr/lib/postgresql/$V/bin/pg_ctl stop -m fast -D $DATADIR/data ; rm -rf $DATADIR" EXIT



echo "******************************************************************************"
echo "*** LOADING SCHEMA ***********************************************************"
echo "******************************************************************************"
echo ""

psql < ../../../access/schema/rawrepo.sql 
psql < ../../../access/schema/queuerules.sql

echo "******************************************************************************"
echo "*** LOADING DATA *************************************************************"
echo "******************************************************************************"
echo ""

for file in data/*; do
    echo "$file"
    name="${file/#data\/???-}"
    name="${name%.*}"
    IFS=- read -r -a args <<<"$name";
    case "$file" in
	*.sql)
	    psql < "$file"
	    ;;
	*.txt)
	    cat "$file"
	    ;;
	*.xml)
	    java -jar ../../../agency-load/target/rawrepo-agency-load.jar \
		 --db="$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE" \
		 ${args[1]:+--parent-agencies="${args[1]}"} "$file"
	    ;;
	*.bash)
	    DB="$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDATABASE" \
	      bash "$file"
	    ;;
	*~)
	    true
	    ;;
	*)
	    echo ""
	    echo "*** UNKNOWN FILE TYPE"
	    echo ""
	    ;;
    esac
done


echo "******************************************************************************"
echo "*** DUMPING DATA *************************************************************"
echo "******************************************************************************"
echo ""

DESTPATH=`cd ../test/resources && pwd`
for table in records records_archive relations queueworkers queuerules; do
    echo "COPY $table TO '$DESTPATH/$table.dump';"
done | psql

