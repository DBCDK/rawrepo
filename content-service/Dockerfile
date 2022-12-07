FROM docker-dbc.artifacts.dbccloud.dk/payara5-full:latest

ADD target/*.war app.json deployments/

LABEL RAWREPO_DB_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver:port/dbname'. (Required)"
LABEL VIPCORE_ENDPOINT="Full url to vipcore service api (Required)"
LABEL VIPCORE_CACHE_AGE="Vipcore cache age. Default 8 hours"
