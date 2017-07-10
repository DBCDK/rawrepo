#!/bin/bash
#set -x

USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble
#AGENCY=870970
#RECID=06683054
FILE=""
FILEPATH=""
VERBOSE=false
us_conn=`egrep updateservice.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2-`
SERVICE=${us_conn}/CatalogingUpdateServices/UpdateService
SERVICE=${us_conn}/UpdateService/2.0

die() {
		echo "ERROR: $@"
		echo "rr_upload.sh ar[mdps]"
		echo "Sending update requests to a updateservice"
		echo "-s <service>    name of the updateservice - default updateservice.url from ${HOME}/.ocb-tools/testrun.properties"
		echo "-f <file>       name of file containing the record"
		echo "-p <path>       path to a folder containing files with names <agencyid>.<recordid> - all such files in the folder will be sent"
		echo "-v <verbose>    more info if true - defalult false"
		exit 1
}

while getopts "s:f:p:v" opt; do
    case "$opt" in
    "s" )
            SERVICE=$OPTARG
            ;;
    "f" )
            FILE="$OPTARG"
            ;;
    "p" )
            FILEPATH="$OPTARG"
            ;;
    "v" )
            VERBOSE="$OPTARG"
            ;;
    esac
done

doit()
{
		file=$1
		HEAD=`echo "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:cat=\"http://oss.dbc.dk/ns/catalogingUpdate\">"\
				"<soapenv:Header/>"\
				"<soapenv:Body>"\
				"<cat:updateRecord>"\
				"<cat:updateRecordRequest>"\
				"<cat:authentication>"\
				"<cat:groupIdAut>010100</cat:groupIdAut>"\
				"<cat:passwordAut>20Koster</cat:passwordAut>"\
				"<cat:userIdAut>netpunkt</cat:userIdAut>"\
				"</cat:authentication>"\
				"<cat:schemaName>allowall</cat:schemaName>"\
				"<cat:bibliographicRecord>"\
				"<cat:recordSchema>info:lc/xmlns/marcxchange-v1</cat:recordSchema>"\
				"<cat:recordPacking>xml</cat:recordPacking>"\
				"<cat:recordData>"\
				"<marcx:collection xmlns:marcx=\"info:lc/xmlns/marcxchange-v1\">"`

		TAIL=`echo "</marcx:collection>"\
				"</cat:recordData>"\
				"</cat:bibliographicRecord>"\
				"<cat:trackingId>${file}</cat:trackingId>"\
				"</cat:updateRecordRequest>"\
				"</cat:updateRecord>"\
				"</soapenv:Body>"\
				"</soapenv:Envelope>"`

		BODY=`cat ${file}`
		echo "${HEAD}${BODY}${TAIL}" > wut.xml
		#curl -s -H "Content-Type: text/xml; charset=utf-8" -H "SOAPAction:"  -d @$TESTNAME.xml -X POST $UPDATE_URL > $OUTPUT_DIR/$TESTNAME.updateResponse.xml
		curl -s -H "Content-Type: text/xml; charset=utf-8" -H "SOAPAction:" -d "${HEAD}${BODY}${TAIL}" -X POST ${SERVICE} > /tmp/cr.res$$
		cc=$?
		result=`cat /tmp/cr.res$$`
				echo ${result} > wut1.xml
		badres=`echo ${result} | egrep "No such record|INTERNAL_SERVER_ERROR" | wc -l`
		if [ ${badres} -eq 1 -o "${result}" = "" ]
		then
				echo ${result}
		fi

		#rm /tmp/cr.res$$
		echo "ran $1"
}

if [ "${FILE}" == "" -a "${FILEPATH}" == "" ]
then
		die "-f <file> or -p <path> must be specified"
fi
if [ "${FILE}" != "" ]
then
		doit ${FILE}
else
		if [ -d ${FILEPATH} ]
		then
				cd ${FILEPATH}
				files=`ls -1 [0-9][0-9][0-9][0-9][0-9][0-9].*`
				for fil in $files
				do
						doit ${fil}
				done

		else
				echo "Directory ${FILEPATH} does not exist"
		fi
fi


