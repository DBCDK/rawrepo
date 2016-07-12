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
     *                                 (OPTIONAL). Defaults to '/rawrepo-content-service'.
     *
     * config              : Map     : Configuration (string:string)
     *                                 (REQUIRED)
     *
     * ..x-forwarded-for   : String  : Ipv4 list (,;) where a-forwarded-for is accepted
     *                                 (OPTIONAL)
     *
     * ..forsRightsUrl'    : String  : URL to forsrights
     *                                 (OPTIONAL)
     *
     * ..forsRightsConnectTimeout : String : timeout in ms
     *                                 (OPTIONAL)
     *
     * ..forsRightsRequestTimeout : String : timeout in ms
     *                                 (OPTIONAL)
     *
     * ..forsRightsDisabled : Boolean : true/false
     *                                 (OPTIONAL)
     *
     * ..forsRightsName     : String : Right Name
     *                                 (OPTIONAL)
     *
     * ..forsRightsRight    : String : Right "Number"
     *                                 (OPTIONAL)
     *
     * ..searchOrderUrl     : String : openagency URL
     *                                 (OPTIONAL)
     *
     * ..searchOrderConnectTimeout : String : timeout in ms
     *                                 (OPTIONAL)
     *
     * ..searchOrderRequestTimeout : String : timeout in ms
     *                                 (OPTIONAL)
     *
     * db                  : Map     : Map of database connections
     *
     * ..rawrepocontentservice/rawrepo : String  : Name of the database descriptor
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
        return '/rawrepo-content-service'
    }

    protected String getCustomResourceName() {
        return 'rawrepo-content-service'
    }

    protected List<String> getDbNames() {
        return ['rawrepocontentservice/rawrepo'];
    }

    protected  Map<String, ArgumentValidator> getConfigOptional() {
        return ['x-forwarded-for' : ArgumentValidator.ANY,
                'forsRightsUrl' : ArgumentValidator.URL,
                'forsRightsConnectTimeout' : ArgumentValidator.UNSIGNED_NUMBER,
                'forsRightsRequestTimeout' : ArgumentValidator.UNSIGNED_NUMBER,
                'forsRightsDisabled' : ArgumentValidator.BOOL,
                'forsRightsName' : ArgumentValidator.WORD,
                'forsRightsRight' : ArgumentValidator.WORD,
                'searchOrderUrl' : ArgumentValidator.URL,
                'searchOrderConnectTimeout' : ArgumentValidator.UNSIGNED_NUMBER,
                'searchOrderRequestTimeout' : ArgumentValidator.UNSIGNED_NUMBER]
    }

    protected Map<String, String> getJdbcPoolDefaultProperties(String name) {
        return [ 'maxPoolSize': '4',
                 'poolResizeQuantity': '1',
                 'steadyPoolSize': '1']
    }
}
