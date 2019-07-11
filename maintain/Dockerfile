FROM docker.dbc.dk/payara5-full:latest

ADD target/*.war app.json deployments/

LABEL OPENAGENCY_URL=""
LABEL INSTANCE_NAME="Name of the maintain instance. Default is blank (Optional)"
LABEL RAWREPO_DB_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver:port/dbname'. (Required)"