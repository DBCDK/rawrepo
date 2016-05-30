/*
 * dbc-rawrepo-introspect
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-introspect.
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-introspect.  If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.rawrepo.introspect.deploy

import dk.dbc.glu.scripts.GluScriptBase
import org.linkedin.glu.agent.api.ScriptFailedException
import org.linkedin.util.io.resource.Resource
import dk.dbc.glu.utils.glassfish.GlassFishAppDeployer
import dk.dbc.glu.utils.logback.Configuration

class IntrospectDeployScript extends GluScriptBase {

    /**************************************************************************
     * Script initParameters
     *
     * NAME                : TYPE    : DESCRIPTION
     *
     * artifact            : String  : An URL pointing to the introspect-service distribution
     *                                 (REQUIRED).
     *
     * contextPath         : String  : The context path to deploy to
     *                                 (OPTIONAL). Defaults to '/rawrepointrospect'.
     *
     * jdbcResources       : Map     : Map of "resource-name" to connect-uri
     *                                 (user:pass@host[:port]/base)
     *                                 (REQUIRED).
     *
     * glassfishProperties : Map     : Glassfish properties port, username and
     *                                 password used for localhost deploy
     *                                 (OPTIONAL).
     *                                 
     * jdbcPoolProperties  : Map     : Properties for the connection pool.
     *                                 See full list of properties in GlassFishAppDeployer
     *                                 (OPTIONAL)   
     *                                 
     * logging          : Map       : Map for configuring logging
     *                                (REQUIRED).
     *                                
     * ..dir            : String    : Path to directory in which log files are stored 
     *                                (REQUIRED).                  
     *                                              
     * ..plain          : String    : Log level for plain log files
     *                                {OFF, ERROR, WARN, INFO, DEBUG, TRACE}
     *                                (REQUIRED). 
     *                                
     * ..logstash       : String    : Log level for logstash log files
     *                                {OFF, ERROR, WARN, INFO, DEBUG, TRACE}
     *                                (REQUIRED).
     *
     *************************************************************************/

    /*******************************************************
     * Script state
     *******************************************************/

    // the following fields represent the state of the script and will be exported to ZooKeeper
    // automatically thus will be available in the console or any other program 'listening' to
    // ZooKeeper

    static String JDBC_BASE = "jdbc/rawrepointrospect"

    /** Webapp .war file assembly area
     */
    Resource stagingArea

    /** Log files folder
     */
    Resource logFolder

    String contextPath

    Resource warFile

    Map jdbcResources = [:]
    
    /** GlassFish information
     */
    Map glassfishProperties = [
        'port': '8080',
        'username': 'admin',
        'password': 'admin',
        'insecure': false,
    ]
    
    Map jdbcPoolProperties = [
        'maxPoolSize': 2,
        'steadyPoolSize': 0
    ]

    // Must be transient or glu will attemp to call getDeployer during installation when enumerating script properties
    transient def deployer

    def getDeployer() {
      if (deployer == null) {
        deployer = new GlassFishAppDeployer(glassfishProperties)
      }
      return deployer;
    }

    /*******************************************************
     * install phase
     *******************************************************/

    def install = {
        log.info "Installing..."

        appName = 'raw-repo-introspect'
        appVersion = '1.0'

        rootFolder = shell.mkdirs( shell.toResource( mountPoint.getPath() ) )

        // Create directories
        logFolder = installLogging( params.logging )

        // fetch artifact
        def webappsFolder = shell.mkdirs( rootFolder."webapps" )
        artifact = fetchResourceIntoFolder( params.artifact, webappsFolder )
        log.info "Using artifact ${artifact.file}"

        
        if (!params.jdbcResources) {
            shell.fail("Required parameter 'jdbcResources' is missing")
        }
        jdbcResources = [:] + params.jdbcResources

        
        // Sets up .war file staging area
        stagingArea = shell.mkdirs( rootFolder."staging" )
        shell.unzip( artifact, stagingArea )

        warFile = webappsFolder."${appName}.war"

        // override default jdbcPoolProperties with init parameters
        jdbcPoolProperties += params.jdbcPoolProperties ?: [:]
        
        // override default glassfishProperties with init parameters
        glassfishProperties += params.glassfishProperties ?: [:]

        if (glassfishProperties.insecure) {
            log.debug "Deploy with HTTP"
            glassfishProperties.baseUrl = "http://localhost:$glassfishProperties.port"
        } else {
            log.debug "Deploy with HTTPS"
            glassfishProperties.baseUrl = "https://localhost:$glassfishProperties.port"
            // Overwrite insecure flag, so CURL does not check certificates
            glassfishProperties.insecure = true
        }
    }

    /*******************************************************
     * configure phase
     *******************************************************/

    def configure = {
        log.info "Configuring..."

        contextPath = params.contextPath ?: "/rawrepointrospect"
        appName = contextPath.substring(1);

        // Modify configurations
        configureLogging()
        configureEjbXml()
        configureJdbcResources()

        // Re-assemble .war archive
        log.info "Re-packaging webapp to ${warFile.file} "
        jar( stagingArea, warFile )
        getDeployer().deploy([
                name: appName,
                id: warFile.file.getPath(),
                contextroot: contextPath,
                enabled: 'false',
                force: 'true'
            ])
    }

    /*******************************************************
     * start phase
     *******************************************************/

    def start = {
        getDeployer().start(appName)
    }

    /*******************************************************
     * stop phase
     *******************************************************/

    def stop = {
        getDeployer().stop(appName)
    }

    /*******************************************************
     * unconfigure phase
     *******************************************************/

    def unconfigure = {
        getDeployer().undeploy(appName)
        deleteJdbcResources()
    }

    /*******************************************************
     * uninstall phase
     *******************************************************/

    def uninstall = {
        log.info "Uninstalling..."

        shell.rmdirs( rootFolder )
    }

    /*******************************************************
     * helper functions
     *******************************************************/

    /**
     * Writes webapp context file to staging area
     */
    def configureEjbXml() {
        def ejbXmlFile = stagingArea."WEB-INF"."ejb-jar.xml"
        log.info "Writing web ${ejbXmlFile.file}"

        def xmlString = """\
<?xml version="1.0" encoding="UTF-8"?>
<ejb-jar xmlns="http://xmlns.jcp.org/xml/ns/javaee"
        version="3.2"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://xmlns.jcp.org/xml/ns/javaee http://xmlns.jcp.org/xml/ns/javaee/ejb-jar_3_2.xsd">
  <enterprise-beans>
    <session>
      <ejb-name>Introspect</ejb-name>
      <env-entry>
        <env-entry-name>jdbcResourceBase</env-entry-name>
        <env-entry-type>java.lang.String</env-entry-type>
        <env-entry-value>$JDBC_BASE</env-entry-value>
      </env-entry>
    </session>
  </enterprise-beans>
</ejb-jar>
"""
        shell.saveContent( ejbXmlFile, xmlString )
    }
    /**
     * Writes log4j.xml configuration file to config folder
     *
     * The actual content of the configuration depends on
     * the state of the <code>params.logging</code>.
     */
    def configureLogging() {
        def logConfigFile = shell.toResource( stagingArea."WEB-INF"."classes"."logback.xml" )
        Configuration logConfig = defaultLogbackConfig()
        registerLogFiles( logConfig )
        logConfig.save( logConfigFile.file ) 
    }

    
    def configureJdbcResources() {

        for(jdbcEntry in jdbcResources) {
            def resource = jdbcEntry.key
            if(! (resource =~ "^[-a-z0-9]+\$")) {
                shell.fail("Required parameter 'jdbcResources' has invalid resource name $resource")
            }
            def url = jdbcEntry.value
            def matcher = url =~ "^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)/(.*)?)\$"
            if(! matcher) {
                shell.fail("Required parameter 'jdbcResources' has invalid value for '$resource' : " + url)
            }
            def m = matcher[0]
            def jdbc = ((m[1] == null ? "jdbc:postgresql://" : m[1]) + m[4]).replace(":", "\\:");
            def user = (m[2] == null ? "" : m[2]).replace(":", "\\:")
            def pass = (m[3] == null ? "" : m[3]).replace(":", "\\:")
            
            getDeployer().createPGConnectionPool(JDBC_BASE + "/" + resource + "/pool", JDBC_BASE + "/" + resource, user, pass, jdbc, jdbcPoolProperties)
        }
    }

    def deleteJdbcResources() {
        for(jdbcEntry in jdbcResources) {
            def resource = jdbcEntry.key
            getDeployer().deleteJdbcResource(JDBC_BASE + "/" + resource)
            getDeployer().deleteJdbcConnectionPool(JDBC_BASE + "/" + resource + "/pool")
        }
    }

}
