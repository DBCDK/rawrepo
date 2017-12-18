#!/bin/bash
#set -x

USER=${USER:-WHAT}    # silencing annoying intellij syntax quibble
#AGENCY=870970
#RECID=06683054
FILE=""
FILEPATH=""
VERBOSE="false"
us_conn=`egrep updateservice.url ${HOME}/.ocb-tools/testrun.properties | tr -d " " | cut -d"=" -f2-`
SERVICE=${us_conn}/CatalogingUpdateServices/UpdateService
SERVICE=${us_conn}/UpdateService/2.0
SCHEMA=allowall
LIBRARY="010100"

die() {
		echo "ERROR: $@"
		echo "rr_upload.sh ar[mdps]"
		echo "Sending update requests to a updateservice"
		echo "-s <service>    name of the updateservice - default updateservice.url from ${HOME}/.ocb-tools/testrun.properties"
		echo "-S <schemaname> name of the schema - default allowall"
		echo "-l <library>    name of the library updating the record - default 010100"
		echo "-f <file>       name of file containing the record"
		echo "-p <path>       path to a folder containing files with names <agencyid>.<something> - all such files in the folder will be sent"
		echo "                Files will typically come from a dump made by content_service_get_rec.sh which creates filenames on the form <agencyid>.<recordid>"
		echo "-v <verbose>    more info if true - default false"
		exit 1
}

while getopts "s:S:l:f:p:v" opt; do
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
    "l" )
            LIBRARY="$OPTARG"
            ;;
    "S" )
            SCHEMA="$OPTARG"
            ;;
    "v" )
            VERBOSE="true"
            ;;
    esac
done

doit()
{
		file=$1
		VERBOSE=$2
		HEAD=`echo "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:cat=\"http://oss.dbc.dk/ns/catalogingUpdate\">"\
				"<soapenv:Header/>"\
				"<soapenv:Body>"\
				"<cat:updateRecord>"\
				"<cat:updateRecordRequest>"\
				"<cat:authentication>"\
				"<cat:groupIdAut>${LIBRARY}</cat:groupIdAut>"\
				"<cat:passwordAut>20Koster</cat:passwordAut>"\
				"<cat:userIdAut>netpunkt</cat:userIdAut>"\
				"</cat:authentication>"\
				"<cat:schemaName>${SCHEMA}</cat:schemaName>"\
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

        xmllint ${file}
		cc=$?
		if [ ${cc} -ne 0 ]
		then
		    echo "Fejl i xml"
		    exit 1
		fi
		BODY=`cat ${file}`
		if [ "${VERBOSE}" == "true" ]
		then
		    echo "${HEAD}${BODY}${TAIL}" >> rr_upload_request.xml
		fi
		#curl -s -H "Content-Type: text/xml; charset=utf-8" -H "SOAPAction:"  -d @$TESTNAME.xml -X POST $UPDATE_URL > $OUTPUT_DIR/$TESTNAME.updateResponse.xml
		curl -s -H "Content-Type: text/xml; charset=utf-8" -H "SOAPAction:" -d "${HEAD}${BODY}${TAIL}" -X POST ${SERVICE} > /tmp/cr.res$$
		cc=$?
		if [ ${cc} -ne 0 ]
		then
		    echo "curl returns ${cc}"
		    cat /tmp/cr.res$$
		fi
		result=`cat /tmp/cr.res$$`
		if [ "${VERBOSE}" == "true" ]
		then
			echo ${result} >> rr_upload_response.xml
		fi
		badres=`echo ${result} | egrep "<updateStatus>ok</updateStatus>" | wc -l`
		if [ ${badres} -eq 0 -o "${result}" = "" ]
		then
		    echo "Failure $1"
			echo ${result}
		fi
		rm /tmp/cr.res$$

		echo "Success $1"
}

if [ "${FILE}" == "" -a "${FILEPATH}" == "" ]
then
		die "-f <file> or -p <path> must be specified"
fi
if [ "${FILE}" != "" ]
then
        if [ "${VERBOSE}" == "true" ]
        then
            WHERE=`pwd`
		    echo "Request can be found in file ${WHERE}/rr_upload_request.xml"
		    echo "Response can be found in file ${WHERE}/rr_upload_response.xml"
            rm rr_upload_request.xml
            rm rr_upload_response.xml
        fi
		doit ${FILE} ${VERBOSE}
else
		if [ -d ${FILEPATH} ]
		then
				cd ${FILEPATH}
           		if [ "${VERBOSE}" == "true" ]
           		then
                    WHERE=`pwd`
		            echo "Request can be found in file ${WHERE}/rr_upload_request.xml"
		            echo "Response can be found in file ${WHERE}/rr_upload_response.xml"
                    rm rr_upload_request.xml
                    rm rr_upload_response.xml
                fi
				files=`ls -1 [0-9][0-9][0-9][0-9][0-9][0-9].*`
				for fil in ${files}
				do
						doit ${fil} ${VERBOSE}
				done

		else
				echo "Directory ${FILEPATH} does not exist"
		fi
fi


