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
     *                                 (OPTIONAL). Defaults to '/rawrepo-maintain'.
     *
     * config              : Map     : Configuration (string:string)
     *                                 (REQUIRED)
     *
     * ..openAgencyUrl     : URL     : Url of openagency >= 1.19 (remember trailing /)
     *                                 (REQUIRED)
     *
     * ..redirect-queue-provider : URL  : Url for queue provider info
     *                                 (REQUIRED)
     *
     * db                  : Map     : Map of database connections
     *
     * ..rawrepomaintain/rawrepo : String  : Name of the database descriptor
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
        return "/rawrepo-maintain"
    }

    protected String getCustomResourceName() {
        return "rawrepo-maintain"
    }

    protected List<String> getDbNames() {
        return ['rawrepomaintain/rawrepo'];
    }

    protected  Map<String, ArgumentValidator> getConfigRequired() {
        return ['openAgencyUrl' : ArgumentValidator.URL,
                'redirect-queue-provider': ArgumentValidator.URL]
    }

    protected Map<String, String> getJdbcPoolDefaultProperties(String name) {
        return [ 'maxPoolSize': '2',
                 'poolResizeQuantity': '1',
                 'steadyPoolSize': '1']
    }
}
