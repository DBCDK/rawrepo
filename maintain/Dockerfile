FROM docker.dbc.dk/payara5-full:latest

ADD target/*.war app.json deployments/

LABEL OPENAGENCY_URL="Full url to openagency service (Required)"
LABEL OPENAGENCY_CONNECT_TIMEOUT="Openagency connect timeout. Default 2500"
LABEL OPENAGENCY_REQUEST_TIMEOUT="Openagency request timeout. Default 10000"
LABEL OPENAGENCY_CACHE_AGE="Openagency cache age. Default 8 hours"

LABEL INSTANCE_NAME="Name of the maintain instance. Default is blank (Optional)"
LABEL RAWREPO_DB_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver:port/dbname'. (Required)"