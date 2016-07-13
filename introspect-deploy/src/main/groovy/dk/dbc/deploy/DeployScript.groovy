
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
     * db                  : Map     : Map of database connections
     * 
     * ..rawrepointrospect/* : String  : Name of the database descriptor
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
        return "/rawrepo-introspect";
    }
    
    protected List<String> getDbNames() {
        return [] + params.db.keySet().findAll{it -> it.startsWith("rawrepointrospect/")}
    }
    
    protected String getCustomResourceName() {
        return "rawrepo-introspect";
    }
    
    protected Map<String, String> getJdbcPoolDefaultProperties(String name) {
        return [ 'maxPoolSize': '4',
                 'poolResizeQuantity': '1',
                 'steadyPoolSize': '1']
    }
}