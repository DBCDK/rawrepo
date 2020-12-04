FROM docker.dbc.dk/payara5-full:latest

ADD target/*.war app.json deployments/

LABEL RAWREPO_DB_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver:port/dbname'. (Required)"
LABEL VIPCORE_ENDPOINT="Full url to vipcore service api (Required)"
LABEL VIPCORE_CACHE_AGE="Vipcore cache age. Default 8 hours"
LABEL FORSRIGHTS_DISABLED="True|False - Indicates if authentication should be disabled. Default false"
LABEL FORSRIGHTS_URL="Full url to forsright service. Required if FORSRIGHTS_DISABLED is false"
LABEL FORSRIGHTS_CONNECT_TIMEOUT="Forsright connect timeout. Default 2500"
LABEL FORSRIGHTS_REQUEST_TIMEOUT="Forsrights request timeout. Default 10000"
LABEL FORSRIGHTS_CACHE="Forsright cache timeout. Default 7200"
LABEL FORSRIGHTS_NAME="The name to lookup in forsrights. Default '*'"
LABEL FORSRIGHTS_RIGHT="The right to lookup in forsrights. Default '*'"