/*
 * dbc-rawrepo-maintain
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-maintain.
 *
 * dbc-rawrepo-maintain is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-maintain is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-maintain.  If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.deploy

import dk.dbc.glassfish.deploy.ArgumentValidator
import dk.dbc.glassfish.deploy.GlassFishWebserviceDeployer

class DeployScript extends GlassFishWebserviceDeployer {
    /**************************************************************************
     * Script initParameters
     *
     * NAME                : TYPE    : DESCRIPTION
     *
     * artifact            : String  : An URL pointing to the maintain distribution
     *                                 (REQUIRED).
     *
     * contextPath         : String  : The context path to deploy to
     *                                 (OPTIONAL). Defaults to '/rawrepo-indexer'.
     *
     * config              : Map     : Configuration (string:string)
     *                                 (REQUIRED)
     *
     * ..solrUrlRawrepo    : URL     : Url of solr-server-core
     *                                 (REQUIRED)
     *
     * ..workerName        : String  : Name of queue to take jobs from
     *                                 (Optional)
     *
     * ..timeout           : Integer : How often to check queue
     *                                 (Optional)
     *
     * ..maxConcurrent     : Integer : How many concurrend workers
     *                                 (Optional)
     *
     * db                  : Map     : Map of database connections
     *
     * ..rawrepoindexer/rawrepo : String  : Name of the database descriptor
     *                                 (REQUIRED).
     *
     * jdbcPoolProperties  : Map     : Jdbc properties
     *                                 (see GlassFishAppDeployer#createJdbcConnectionPool)
     *                                 (OPTIONAL)
     *
     * logging             : Map     : Map for configuring logging
     *                                 (REQUIRED).
     *
     * ..dir               : String  : Path to directory in which log files are stored
     *                                 (REQUIRED).
     *
     * ..plain             : String  : Log level for plain log files
     *                                 {OFF, ERROR, WARN, INFO, DEBUG, TRACE}
     *                                 (REQUIRED).
     *
     * ..logstash          : String  : Log level for logstash log files
     *                                 {OFF, ERROR, WARN, INFO, DEBUG, TRACE}
     *                                 (REQUIRED).
     *
     * glassfishProperties : Map     : Glassfish properties port, username and
     *                                 password used for localhost deploy
     *                                 (OPTIONAL).
     *
     *************************************************************************/



    protected String getDefaultContextPath() {
        return "/rawrepo-indexer"
    }

    protected String getCustomResourceName() {
        return "rawrepo-indexer"
    }

    protected List<String> getDbNames() {
        return ['rawrepoindexer/rawrepo'];
    }

    protected Map<String, String> getConfigDefaultValues() {
        return ['workerName': 'solr-sync',
                'timeout': '5',
                'maxConcurrent': '4']
    }

    protected  Map<String, ArgumentValidator> getConfigRequired() {
        return ['solrUrlRawrepo' : ArgumentValidator.URL]
    }

    protected  Map<String, ArgumentValidator> getConfigOptional() {
        return ['workerName': ArgumentValidator.ANY,
                'timeout': ArgumentValidator.UNSIGNED_NUMBER,
                'maxConcurrent': ArgumentValidator.UNSIGNED_NUMBER]
    }

    protected Map<String, String> getJdbcPoolDefaultProperties(String name) {
        return [ 'maxPoolSize': '8',
                 'poolResizeQuantity': '1',
                 'steadyPoolSize': '1']
    }
}
