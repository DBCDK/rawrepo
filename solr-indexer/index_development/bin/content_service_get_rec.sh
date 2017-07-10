#!/bin/bash
#set -x

USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble
#AGENCY=870970
#RECID=06683054
AGENCY=""
RECID=""
FILE=""
IDFILE=""
MODE=RAW
DELETED=false
PRIVATE=true
VERBOSE=false
SERVICE=devel8:`id -u ${USER}`2/RawRepoContentService

die() {
		echo "ERROR: $@"
		echo "content_service_get_rec.sh ar[mdpsfiv]"
		echo "Find and write out a record from a RR"
		echo "-a <agency>     Agency id for the record"
		echo "-r <recordid>   Id for the wanted record"
		echo "-m <MODE>       RAW|MERGED|COLLECTION - default RAW"
		echo "-d <deleted>    false: don't fetch deleted, true:fetch deleted - default false"
		echo "-p <private>    true:include enrichment, false: only common - default true"
		echo "-s <service>    name of the contentservice - default devel8:`id -u ${USER}`2/RawRepoContentService"
		echo "-f <file>       output file for fetched records - output will be appended - default output will be written to file agency.recordid"
		echo "-i <file>       file with agency:recordid lines - output will be treated according to -f"
		echo "-v <verbose>    more info if true - defalult false"
		exit 1
}

while getopts "a:r:m:d:p:s:f:i:v" opt; do
    case "$opt" in
    "a" )
            AGENCY=$OPTARG
            ;;
    "r" )
            RECID=$OPTARG
            ;;
    "m" )
            MODE=$OPTARG
            ;;
    "d" )
            DELETED=$OPTARG
            ;;
    "p" )
            PRIVATE=$OPTARG
            ;;
    "s" )
            SERVICE=$OPTARG
            ;;
    "f" )
            FILE="$OPTARG"
            ;;
    "i" )
            IDFILE="$OPTARG"
            ;;
    "v" )
            VERBOSE="$OPTARG"
            ;;
    esac
done

doit()
{
		agency=$1
		recordid=$2
		REQUEST=`echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><S:Envelope xmlns:S=\"http://schemas.xmlsoap.org/soap/envelope/\">" \
				"<S:Body><fetchRequest xmlns=\"http://oss.dbc.dk/ns/rawreposervice\">" \
				"<records><record><bibliographicRecordId>${recordid}</bibliographicRecordId><agencyId>${agency}</agencyId><mode>${MODE}</mode>" \
				"<allowDeleted>${DELETED}</allowDeleted><includeAgencyPrivate>${PRIVATE}</includeAgencyPrivate></record></records>" \
				"</fetchRequest></S:Body></S:Envelope>"`

		curl -s -H "Content-Type: text/xml; charset=utf-8" -H "SOAPAction:" -d "${REQUEST}" -X POST ${SERVICE} > /tmp/cr.res$$
		cc=$?
		result=`cat /tmp/cr.res$$`
		badres=`echo ${result} | egrep "No such record|INTERNAL_SERVER_ERROR" | wc -l`
		if [ ${badres} -eq 1 -o "${result}" = "" ]
		then
				echo "Could not find ${agency}:${recordid} delete: ${DELETED} private: ${PRIVATE} mode: ${MODE}"
				if [ "${VERBOSE}" = "true" ]
				then
						echo ${result}
				fi
		else
				#cat ud | sed -e 's/<data>/<data>\nbase64=/' -e 's/<\/data>/\n<\/data>/' | egrep '<data>'

				base64_res=`echo ${result} | sed -e 's/<data>/<data>\nbase64=/' -e 's/<\/data>/\n<\/data>/' | egrep '^base64=' | cut -d"=" -f2- | base64 -d | xmllint --format -`
				if [ "${agency}" = "191919" ]
				then
						agency="870970"
				fi
				if [ "${FILE}" != "" ]
				then
						echo ${base64_res} | xmllint --format - | tail -n+2 | sed -e 's/code="b">191919/code="b">870970/' >> ${FILE}
				else
						echo ${base64_res} | xmllint --format - | tail -n+2 | sed -e 's/code="b">191919/code="b">870970/' > ${agency}.${recordid}
				fi
		fi

		#cat ud | sed -e 's/<data>/<data>\nbase64=/' -e 's/<\/data>/\n<\/data>/' | egrep '</data>'
		rm /tmp/cr.res$$
}

if [ "${IDFILE}" == "" ]
then
		if [ "${AGENCY}" == "" -o "${RECID}" == "" ]
		then
				die "Agency and recid must be specified"
		fi
		doit ${AGENCY} ${RECID}
else
		if [ -r ${IDFILE} ]
		then
				while read pair; do
						ag=`echo ${pair} |cut -d":" -f1`
						id=`echo ${pair} |cut -d":" -f2`
						doit ${ag} ${id}
				done < ${IDFILE}
		else
				echo "File ${IDFILE} cannot be read"
		fi
fi


