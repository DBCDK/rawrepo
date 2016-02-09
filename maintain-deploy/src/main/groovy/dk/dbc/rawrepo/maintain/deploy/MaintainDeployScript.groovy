/*
 * This file is part of opensearch.
 * Copyright (c) 2012, Dansk Bibliotekscenter a/s,
 * Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043
 *
 * opensearch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * opensearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with opensearch. If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.rawrepo.maintain.deploy

import dk.dbc.glu.scripts.GluScriptBase
import dk.dbc.glu.utils.glassfish.GlassFishAppDeployer
import dk.dbc.glu.utils.logback.Appender
import dk.dbc.glu.utils.logback.Configuration
import dk.dbc.glu.utils.logback.Format
import dk.dbc.glu.utils.logback.Level
import dk.dbc.glu.utils.logback.Logger
import org.linkedin.util.io.resource.Resource

class MaintainDeployScript extends GluScriptBase {

    /**************************************************************************
     * Script initParameters
     *
     * NAME                : TYPE    : DESCRIPTION
     *
     * artifact            : String  : An URL pointing to the maintain distribution
     *                                 (REQUIRED).
     *
     * contextPath         : String  : The context path to deploy to
     *                                 (OPTIONAL). Defaults to '/rawrepomaintain'.
     *
     * config              : Map     : Configuration
     *                                 (REQUIRED)
     * 
     *  .openAgencyUrl     : String  : Url of openagency >= 1.19 (remember trailing /)
     *                                 (REQUIRED)
     * 
     * .redirect-queue-provider: String : Url of queue provider doc (not defined -> 404)
     *                                 (OPTIONAL)
     * 
     * db                  : List>Map: List of database descriptors
     *                                 Config named 'rawrepo' REQUIRED
     * 
     * ..name              : String  : Name of the database descriptor
     *                                 (REQUIRED).
     * 
     * ..url               : String  : Base url of the raw repo database
     *                                 (REQUIRED).
     *
     * ..user              : String  : Username for the raw repo database
     *                                 (REQUIRED).
     *
     * ..password          : String  : Password url of the raw repo database
     *                                 (REQUIRED).
     *                                 
     * ..validateInterval  : Integer : How often (sec) to validate the connection
     *                                 (REQUIRED).
     *                                 
     * logDir              : String  : Location of logging
     *                                 (OPTIONAL)
     * 
     * loggers             : List>Map: List of logger maps
     *                                 (OPTIONAL)
     * 
     *  ..name             : String  : name of logger
     *                                 (OPTIONAL)
     * 
     *  ..file             : String  : prefix of filename (appended $type.log)
     *                                 (REQUIRED) 
     *                                 
     *  ..level            : String  : ERROR|WARN|INFO|DEBUG|TRACE
     *                                 (OPTIONAL)
     * 
     *  ..format           : String  : PLAIN|LOGSTASH
     *                                 (OPTIONAL)
     *  
     *  ..rotate           : Boolean : Logrotate
     *                                 (OPTIONAL)
     * 
     * glassfishProperties : Map     : Glassfish properties port, username and
     *                                 password used for localhost deploy
     *                                 (OPTIONAL).
     *                                 
     *************************************************************************/

    static String NAME = "rawrepo-maintain"
    static String CUSTOM_RESOURCE_NAME = "rawrepo-maintain"
    static String OPEN_AGENCY_URL = "openAgencyUrl"

    static String RAWREPO_NAME = "rawrepo"
    static String RAWREPO_RESOURCE = "jdbc/rawrepomaintain/rawrepo"
    static String RAWREPO_POOL = "jdbc/rawrepomaintain/rawrepo/pool"

    Resource logFolder
    Resource stagingFolder
    Resource warFile
    String contextPath

    Map customProperties = [:]

    /*
     * * GlassFish information
     */
    Map glassfishProperties = [
        'port': '8080',
        'username': 'admin',
        'password': 'admin',
        'insecure': false,
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

        appName = NAME
        appVersion = '1.0-SNAPSHOT'

        contextPath = params.contextPath ?: "/" + appName
        appName = contextPath.substring(1);

        [ 'config' ].each({
                if (!params."$it") {
                    shell.fail("Required parameter '$it' is missing")
                }
            })
        
        customProperties += params.config
        [ OPEN_AGENCY_URL ].each({
                if (!customProperties."$it") {
                    shell.fail("Required parameter 'config.$it' is missing")
                }
            })

        validateDatabases( RAWREPO_NAME );
        
        validateLoggers()
        
        rootFolder = shell.mkdirs( shell.toResource( mountPoint.getPath() ) )
        log.info "rootFolder: ${rootFolder}"
        
        logFolder = shell.mkdirs( params.logDir ?: rootFolder."log-files" )
        shell.chmod(logFolder, "a+w")
        log.info "Created log files folder: ${logFolder.file}"

        def webappsFolder = shell.mkdirs( rootFolder."webapps" )
        def artifact = fetchResourceIntoFolder( params.artifact, webappsFolder )
        log.info "Using artifact ${artifact.file}"

        stagingFolder = shell.mkdirs( rootFolder."staging" )
        shell.unzip( artifact, stagingFolder )

        warFile = webappsFolder."${appName}.war"

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
    
    void validateLoggers() {
        if(params.loggers) {
            params.loggers.each({
                    Map map = it;
                    [ 'file' ].each({
                            if(!map."$it") 
                            shell.fail("Required parameter 'loggers..${it}' is missing")
                        })
                })
        }
    }
    
    void validateDatabases(String... requiredDbNames) {
        if(!params.db && requiredDbNames) {
            shell.fail("Required parameter 'db' is missing")
        }
        params.db.each({
                Map db = it;
                [ 'name', 'url', 'user', 'password', 'validateInterval' ].each({
                        if(db."$it" == null) {
                            shell.fail("Required parameter 'db.[@name=${db.name}].${it}' is missing")
                        }
                    })
            })
        requiredDbNames.each({
                def name = it;
                if(params.db.count({ it.'name' == name }) == 0) {
                    shell.fail("Required parameter 'db..[name=$name] is missing")
                }
            })
    }

    /*******************************************************
     * configure phase
     *******************************************************/
    def configure = {
        log.info "Configuring..."
        
        configureLogging()
        configureJdbcResource( RAWREPO_NAME, RAWREPO_POOL, RAWREPO_RESOURCE )
        configureCustomResource( CUSTOM_RESOURCE_NAME, customProperties )

        log.info "Re-packaging webapp to ${warFile.file}"
        jar( stagingFolder, warFile )
        getDeployer().deploy([
                name: appName,
                id: warFile.file.getPath(),
                contextroot: contextPath,
                enabled: 'false',
                force: 'true'
            ])
    }

    Integer unnamedLoggerNo = 1;
    void configureLogging() {
        def logConfigFile = stagingFolder."WEB-INF/classes/logback.xml"
        Configuration logbackConfiguration = basicLogbackConfiguration()
        if(params.logging) {
            params.logging.each({
                    log.info "Adding custom logger $it"
                    String name = it.name ?: ( "unnamedLoggerNo" + unnamedLoggerNo++ )
                    String filePrefix = it.file
                    Level level = ( it.level ?: "info" ).toUpperCase()
                    Format format = ( it.format ?: "plain" ).toUpperCase()
                    Boolean rotate = it.rotate ?: false
                    Appender appender = new Appender(
                        name: name, variant: "out",
                        file: logFolder."${filePrefix}".file,
                        rotate: rotate,
                        format: format,
                        threshold: level )
                    logbackConfiguration.addAppender(appender)
                })
        }
        logbackConfiguration.level = Level.TRACE
        logbackConfiguration.save(logConfigFile.file)
        registerLogFiles(logbackConfiguration)
    }
        
    void configureCustomResource( String name, Map properties ) {
        getDeployer().createCustomResource( [
                'id': name,
                'restype': 'java.util.Properties',
                'factoryclass': 'org.glassfish.resources.custom.factory.PropertiesFactory'
            ])
        getDeployer().setCustomResourceProperties( name, properties )
    }

    void configureJdbcResource(String name, String pool, String resource) {
        def db = params.db.find({ it.'name' == name })
        def url = db.url.replace(":", "\\:");        
        def poolOptions = [
            name: pool,
            resType: "javax.sql.DataSource",
            datasourceClassname: "org.postgresql.ds.PGSimpleDataSource",
            steadypoolsize: "0",
            maxpoolsize: "2",
            property: "\"driverClass=org.postgresql.Driver:url=$url:User=$db.user:Password=$db.password\"",
            isConnectionValidationRequired: "true",
            validateAtmostOncePeriodInSeconds: db.validateInterval,
            connectionValidationMethod: "custom-validation",
            validationClassname: "org.glassfish.api.jdbc.validation.PostgresConnectionValidation",
            failAllConnections: "true"
        ]
        
        def extraPoolOptions = [ 'steadypoolsize', 'maxpoolsize' ]
        poolOptions += db.findAll({ extraPoolOptions.contains( it.key ) })
        
        getDeployer().createJdbcConnectionPool( poolOptions )
        getDeployer().createJdbcResource([ id: resource, poolName: pool ])
    }

    /*******************************************************
     * start phase
     *******************************************************/
    def start = {
        log.info "Starting..."
        
        getDeployer().start( appName )
    }

    /*******************************************************
     * stop phase
     *******************************************************/

    def stop = {
        log.info "Stopping..."
        
        getDeployer().stop( appName )
    }

    /*******************************************************
     * unconfigure phase
     *******************************************************/

    def unconfigure = {
        log.info "Unconfiguring..."
        
        getDeployer().undeploy( appName )
        getDeployer().deleteCustomResource( CUSTOM_RESOURCE_NAME )
        getDeployer().deleteJdbcResource( RAWREPO_RESOURCE )
        getDeployer().deleteJdbcConnectionPool( RAWREPO_POOL )
    }

    /*******************************************************
     * uninstall phase
     *******************************************************/

    def uninstall = {
        log.info "Uninstalling..."

        shell.rmdirs( rootFolder )
    }
}
