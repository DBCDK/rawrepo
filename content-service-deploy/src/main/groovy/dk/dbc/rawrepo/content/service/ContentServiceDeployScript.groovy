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
     * dbUrl               : String  : Base url of the raw repo database
     *                                 (REQUIRED).
     *
     * dbUser              : String  : Username for the raw repo database
     *                                 (REQUIRED).
     *
     * dbPassword          : String  : Password url of the raw repo database
     *                                 (REQUIRED).
     *                                 
     * dbValidateInterval  : Integer : How often (sec) to validate the connection
     *                                 (REQUIRED).
     *                                 
     * config              : Map     : Configuration
     *                                 (REQUIRED)
     * 
     *  .searchOrderUrl    : String  : Url of openagency >= 1.19 (remember trailing /)
     *                                 (REQUIRED)
     * 
     *  .forsRightsDisabled: Boolean : If set to true other config.forsRights* are 
     *                                 not required
     *                                 (OPTIONAL)
     * 
     *  .forsRightsUrl     : String  : Where forsright service is installed
     *                                 (Remember trailing /).
     *                                 (REQUIRED)
     *                                 
     *  .forsRightsName    : String  : Name of rights name
     *                                 (REQUIRED)
     * 
     *  .forsRightsRight   : String  : Name of  rights right
     *                                 (REQUIRED)
     *                                 
     * logDir              : String  : Location of logging
     *                                 (OPTIONAL)
     * 
     * logging             : List>Map: List of logger maps
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
    static String FORS_URL = "forsRightsUrl"
    static String FORS_CACHE = "forsRightsCache"
    static String FORS_DISABLE = "forsRightsDisable"
    static String SEARCHORDER_URL = "searchOrderUrl"

    static String JDBC_RESOURCE = "jdbc/rawrepocontentservice/rewrepo"
    static String JDBC_POOL = "jdbc/rawrepocontentservice/rawrepo/pool"

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
    
    GlassFishAppDeployer deployer

    /*******************************************************
     * install phase
     *******************************************************/
    def install = {
        log.info "Installing..."

        appName = NAME
        appVersion = '1.0-SNAPSHOT'

        contextPath = params.contextPath ?: "/" + appName
        appName = contextPath.substring(1);

        [ "dbUrl", "dbUser", "dbPassword", "dbValidateInterval", "config" ].each({
                if (!params."$it") {
                    shell.fail("Required parameter '$it' is missing")
                }
            })
        
        customProperties  += params.config
        [ "searchOrderUrl" ].each({
                if (!customProperties."$it") {
                    shell.fail("Required parameter 'config.$it' is missing")
                }
            })
        if (!customProperties.forsRightsDisabled) {
            [ "forsRightsUrl", "forsRightsName", "forsRightsRight" ].each({
                    if (!customProperties."$it") {
                        shell.fail("Required parameter 'config.$it' is missing")
                    }
                })
        }
        if (params.logging) {
            params.logging.each({
                    if(!it."file") {
                        shell.fail("Required parameter 'logging..file' is missing")
                    }
            })
        }
        
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

        deployer = new GlassFishAppDeployer( glassfishProperties )

    }
    
    /*******************************************************
     * configure phase
     *******************************************************/
    def configure = {
        log.info "Configuring..."
        
        configureLogging()
        configureCustomResources()
        configureJdbcResources()

        log.info "Re-packaging webapp to ${warFile.file}"
        jar( stagingFolder, warFile )
        deployer.deploy([
                name: appName,
                id: warFile.file.getPath(),
                contextroot: contextPath,
                enabled: 'false',
                force: 'true'
            ])
    }

    void configureLogging() {
        def logConfigFile = stagingFolder."WEB-INF/classes/logback.xml"
        Configuration logbackConfiguration = basicLogbackConfiguration()
        if(params.logging) {
            params.logging.each({
                    log.info "Adding custom logger $it"
                    addLogger(logbackConfiguration, it);
                })
        }
        logbackConfiguration.save(logConfigFile.file)
        registerLogFiles(logbackConfiguration)
    }
    
    Integer unnamedLoggerNo = 1;
    
    void addLogger(Configuration logback, Map cfg) {
        String name = cfg.name ?: ("unnamedLoggerNo" + unnamedLoggerNo++)
        String filePrefix = cfg.file
        Level level = (cfg.level ?: "info").toUpperCase()
        Format format = (cfg.format ?: "plain").toUpperCase()
        Boolean rotate = cfg.rotate ?: false
        Appender appender = new Appender(name: name, variant: "out", file: logFolder."${filePrefix}".file, rotate: rotate, format: format, threshold: level)
        logback.addAppender(appender)
    }
    
    void configureCustomResources() {
        // try { deployer.deleteCustomResource( NAME ) } catch (all) {}
        
        deployer.createCustomResource( [
                'id': NAME,
                'restype': 'java.util.Properties',
                'factoryclass': 'org.glassfish.resources.custom.factory.PropertiesFactory'
            ])
        deployer.setCustomResourceProperties( "rawrepo-content-service", customProperties )
    }

    void configureJdbcResources() {
        // try { deployer.deleteJdbcResource( JDBC_RESOURCE ) } catch (all) {}
        // try { deployer.deleteJdbcConnectionPool( JDBC_POOL ) } catch (all) {}

        def dbUrl = params.dbUrl.replace(":", "\\:");

        def poolOptions = [
            name: JDBC_POOL,
            resType: "javax.sql.DataSource",
            datasourceClassname: "org.postgresql.ds.PGSimpleDataSource",
            //steadypoolsize: "8",
            //maxpoolsize: "32",
            property: "\"driverClass=org.postgresql.Driver:url=$dbUrl:User=$params.dbUser:Password=$params.dbPassword\"",
            isConnectionValidationRequired: "true",
            validateAtmostOncePeriodInSeconds: params.dbValidateInterval,
            connectionValidationMethod: "custom-validation",
            validationClassname: "org.glassfish.api.jdbc.validation.PostgresConnectionValidation",
            failAllConnections: "true"
        ]
        
        deployer.createJdbcConnectionPool(poolOptions)
        deployer.createJdbcResource([
                id: JDBC_RESOURCE,
                poolName: JDBC_POOL,
            ])
    }

    /*******************************************************
     * stop phase
     *******************************************************/
    def start = {
        log.info "Starting..."
        
        deployer.start(appName)
    }

    /*******************************************************
     * stop phase
     *******************************************************/

    def stop = {
        log.info "Stopping..."
        
        deployer.stop(appName)
    }

    /*******************************************************
     * unconfigure phase
     *******************************************************/

    def unconfigure = {
        log.info "Unconfiguring..."
        
        deployer.undeploy(appName)
        deployer.deleteCustomResource( NAME )
        deployer.deleteJdbcResource( JDBC_RESOURCE )
        deployer.deleteJdbcConnectionPool( JDBC_POOL )
    }

    /*******************************************************
     * uninstall phase
     *******************************************************/

    def uninstall = {
        log.info "Uninstalling..."

        shell.rmdirs( rootFolder )
    }
}
