FROM docker-dbc.artifacts.dbccloud.dk/payara5-full:latest

ADD target/*.war app.json deployments/

LABEL VIPCORE_ENDPOINT="Full url to vipcore service (Required)"
LABEL VIPCORE_CACHE_AGE="Vipcore cache age. Default 8 hours"

LABEL INSTANCE_NAME="Name of the maintain instance. Default is blank (Optional)"
LABEL RAWREPO_DB_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver:port/dbname'. (Required)"
