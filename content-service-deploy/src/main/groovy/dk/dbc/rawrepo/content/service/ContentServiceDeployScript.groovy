/*****************************************************************************
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-content-service
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service

import dk.dbc.glu.scripts.GluScriptBase
import dk.dbc.glu.utils.glassfish.GlassFishAppDeployer
import dk.dbc.glu.utils.logback.Appender
import dk.dbc.glu.utils.logback.Configuration
import dk.dbc.glu.utils.logback.Format
import dk.dbc.glu.utils.logback.Level
import dk.dbc.glu.utils.logback.Logger
import org.linkedin.util.io.resource.Resource

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
class ContentServiceDeployScript extends GluScriptBase {

    /**************************************************************************
     * Script initParameters
     *
     * NAME                : TYPE    : DESCRIPTION
     *
     * artifact            : String  : An URL pointing to the content-service distribution
     *                                 (REQUIRED).
     *
     * contextPath         : String  : The context path to deploy to
     *                                 (OPTIONAL). Defaults to '/rawrepo-content-service'.
     *
     * config              : Map     : Configuration
     *                                 (REQUIRED)
     * 
     *  .searchOrderUrl    : String  : Url of openagency >= 1.19 (remember trailing /)
     *                                 (REQUIRED)
     * 
     *  .searchOrderConnectTimeout: Int: Network Option
     *                                 (OPTIONAL)
     *  
     *  .searchOrderRequestTimeout: Int: Network Option
     *                                 (OPTIONAL)
     *                                 
     *  .forsRightsDisabled: Boolean : If set to true other config.forsRights* are 
     *                                 not required
     *                                 (OPTIONAL)
     * 
     *  .forsRightsUrl     : String  : Where forsright service is installed
     *                                 (Remember trailing /).
     *                                 (REQUIRED)
     *  
     *  .forsRightsConnectTimeout: Int: Network Option
     *                                 (OPTIONAL)
     *  
     *  .forsRightsRequestTimeout: Int: Network Option
     *                                 (OPTIONAL)
     *                                 
     *  .forsRightsName    : String  : Name of rights name
     *                                 (REQUIRED)
     * 
     *  .forsRightsRight   : String  : Name of  rights right
     *                                 (REQUIRED)
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
     */

    static String NAME = "rawrepo-content-service"
    static String CUSTOM_RESOURCE_NAME = "rawrepo-content-service"
    static String FORS_URL = "forsRightsUrl"
    static String FORS_CACHE = "forsRightsCache"
    static String FORS_DISABLE = "forsRightsDisable"
    static String SEARCHORDER_URL = "searchOrderUrl"

    static String RAWREPO_NAME = "rawrepo"
    static String RAWREPO_RESOURCE = "jdbc/rawrepocontentservice/rawrepo"
    static String RAWREPO_POOL = "jdbc/rawrepocontentservice/rawrepo/pool"

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
        [ 'searchOrderUrl' ].each({
                if (!customProperties."$it") {
                    shell.fail("Required parameter 'config.$it' is missing")
                }
            })
        if (!customProperties.forsRightsDisabled) {
            [ 'forsRightsUrl', 'forsRightsName', 'forsRightsRight' ].each({
                    if (!customProperties."$it") {
                        shell.fail("Required parameter 'config.$it' is missing")
                    }
                })
        }

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
            //steadypoolsize: "8", maxpoolsize: "32",
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
