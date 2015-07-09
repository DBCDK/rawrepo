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

package dk.dbc.rawrepo.indexer.deploy

import dk.dbc.glu.scripts.GluScriptBase
import org.linkedin.glu.agent.api.ScriptFailedException
import org.linkedin.util.io.resource.Resource
import dk.dbc.glu.utils.glassfish.GlassFishAppDeployer

class IndexerDeployScript extends GluScriptBase {

    /**************************************************************************
     * Script initParameters
     *
     * NAME                : TYPE    : DESCRIPTION
     *
     * artifact            : String  : An URL pointing to the indexer-service distribution
     *                                 (REQUIRED).
     *
     * contextPath         : String  : The context path to deploy to
     *                                 (OPTIONAL). Defaults to '/rawrepoindexer'.
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
     * dbQueueWorker       : String  : Queue worker for dequeueing from database
     *                                 (REQUIRED).
     *
     * solrUrl             : String  : Base url of the Solr service
     *                                 (REQUIRED).
     *
     * pollInterval        : String  : Interval (in seconds) between polling queue for work
     *                                 (REQUIRED).
     *
     * threadPoolSize      : String  : Number of threads that perform indexing
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
     * debugFlag           : Boolean : Flag toggling debug configuration.
     *                                 Configuring for debug mode will for
     *                                 example influence the amount of logging
     *                                 (OPTIONAL).
     *
     *************************************************************************/

    /*******************************************************
     * Script state
     *******************************************************/

    // the following fields represent the state of the script and will be exported to ZooKeeper
    // automatically thus will be available in the console or any other program 'listening' to
    // ZooKeeper

    static String JDBC_RESOURCE = "jdbc/rawrepoindexer/rawrepo"
    static String JDBC_POOL = "jdbc/rawrepoindexer/rawrepo/pool"

    /** Webapp .war file assembly area
     */
    Resource stagingArea

    /** Log files folder
     */
    Resource logFolder

    String contextPath

    Resource warFile

    /** GlassFish information
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

        appName = 'raw-repo-indexer'
        appVersion = '1.0'

        rootFolder = shell.mkdirs( shell.toResource( mountPoint.getPath() ) )

        // Create directories
        logFolder = shell.mkdirs( params.logDir ?: rootFolder."log-files" )
        shell.chmod(logFolder, "a+w")
        log.info "Created log files folder: ${logFolder.file}"

        // fetch artifact
        def webappsFolder = shell.mkdirs( rootFolder."webapps" )
        artifact = fetchResourceIntoFolder( params.artifact, webappsFolder )
        log.info "Using artifact ${artifact.file}"


        ["solrUrl", "dbUrl", "dbUser", "dbPassword", "dbQueueWorker", "pollInterval", "threadPoolSize" ].each({
            if (!params."$it") {
                shell.fail("Required parameter '$it' is missing")
            }
        })

        // Sets up .war file staging area
        stagingArea = shell.mkdirs( rootFolder."staging" )
        shell.unzip( artifact, stagingArea )

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

        deployer = new GlassFishAppDeployer(glassfishProperties)
    }

    /*******************************************************
     * configure phase
     *******************************************************/

    def configure = {
        log.info "Configuring..."

        contextPath = params.contextPath ?: "/rawrepoindexer"
        appName = contextPath.substring(1);

        // Modify configurations
        configureLogging()
        configureEjbXml()
        configureJdbcResources()

        // Re-assemble .war archive
        log.info "Re-packaging webapp to ${warFile.file} "
        jar( stagingArea, warFile )
        deployer.deploy([
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
        deployer.start(appName)
    }

    /*******************************************************
     * stop phase
     *******************************************************/

    def stop = {
        deployer.stop(appName)
    }

    /*******************************************************
     * unconfigure phase
     *******************************************************/

    def unconfigure = {
        deployer.undeploy(appName)
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
      <ejb-name>Indexer</ejb-name>
      <env-entry>
        <env-entry-name>solrUrl</env-entry-name>
        <env-entry-type>java.lang.String</env-entry-type>
        <env-entry-value>$params.solrUrl</env-entry-value>
      </env-entry>
      <env-entry>
        <env-entry-name>workerName</env-entry-name>
        <env-entry-type>java.lang.String</env-entry-type>
        <env-entry-value>$params.dbQueueWorker</env-entry-value>
      </env-entry>
    </session>
    <session>
      <ejb-name>Dispatcher</ejb-name>
      <env-entry>
        <env-entry-name>timeout</env-entry-name>
        <env-entry-type>java.lang.Integer</env-entry-type>
        <env-entry-value>$params.pollInterval</env-entry-value>
      </env-entry>
      <env-entry>
        <env-entry-name>maxConcurrent</env-entry-name>
        <env-entry-type>java.lang.Integer</env-entry-type>
        <env-entry-value>$params.threadPoolSize</env-entry-value>
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
     * the state of the <code>params.debugFlag</code>.
     */
    def configureLogging() {
        if( params.debugFlag ) {
            configureDebugLogging()
        } else {
            configureOperationalLogging()
        }
    }

    /**
     * Writes debug version of log4j.xml configuration file to config folder
     */
    def configureDebugLogging() {
        def logConfigFile = shell.toResource( stagingArea."WEB-INF"."classes"."logback.xml" )

        def logConfig = """\
<?xml version="1.0" encoding="UTF-8" ?>
<configuration>

  <appender name="indexerlog" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${logFolder.file.getPath()}/indexer.log</file>
    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>${logFolder.file.getPath()}/indexer.%i.log</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>10</maxIndex>
    </rollingPolicy>
    <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
       <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS},%p,%c,%t,%C{0},%M %m%n</pattern>
       <immediateFlush>true</immediateFlush>
    </encoder>
  </appender>

  <appender name="indexererror" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${logFolder.file.getPath()}/indexer-error.log</file>
    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>${logFolder.file.getPath()}/indexer-error.%i.log</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>10</maxIndex>
    </rollingPolicy>
    <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
       <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS},%p,%c,%t,%C{0},%M %m%n</pattern>
       <immediateFlush>true</immediateFlush>
    </encoder>
    <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
      <level>ERROR</level>
    </filter>
  </appender>

  <root>
    <level value="TRACE"/>
    <appender-ref ref="indexerlog"/>
    <appender-ref ref="indexererror"/>
  </root>

</configuration>
"""
        log.info "Writing log config in debug mode: ${logConfigFile.file}"
        shell.saveContent( logConfigFile.file, logConfig )
        configureLogFields( logFolder.file.getPath(), logConfig )
    }

    /**
     * Writes operational version of log4j.xml configuration file to config folder
     */
    def configureOperationalLogging() {
        def logConfigFile = shell.toResource( stagingArea."WEB-INF"."classes"."logback.xml" )

        def logConfig = """\
<?xml version="1.0" encoding="UTF-8" ?>
<configuration>

  <appender name="indexerlog" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${logFolder.file.getPath()}/indexer.log</file>
    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>${logFolder.file.getPath()}/indexer.%i.log</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>10</maxIndex>
    </rollingPolicy>
    <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
       <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS},%p,%c,%t,%C{0},%M %m%n</pattern>
       <immediateFlush>true</immediateFlush>
    </encoder>
  </appender>

  <appender name="indexererror" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>${logFolder.file.getPath()}/indexer-error.log</file>
    <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      <maxFileSize>100MB</maxFileSize>
    </triggeringPolicy>
    <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      <fileNamePattern>${logFolder.file.getPath()}/indexer-error.%i.log</fileNamePattern>
      <minIndex>1</minIndex>
      <maxIndex>10</maxIndex>
    </rollingPolicy>
    <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
       <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS},%p,%c,%t,%C{0},%M %m%n</pattern>
       <immediateFlush>true</immediateFlush>
    </encoder>
    <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
      <level>ERROR</level>
    </filter>
  </appender>

  <root>
    <level value="INFO"/>
    <appender-ref ref="indexerlog"/>
    <appender-ref ref="indexererror"/>
  </root>

</configuration>
"""
        log.info "Writing log config in operational mode: ${logConfigFile.file}"
        shell.saveContent( logConfigFile.file, logConfig )
        configureLogFields( logFolder.file.getPath(), logConfig )
    }
    def configureJdbcResources() {

        def dbUrl = params.dbUrl.replace(":", "\\:");

        def poolOptions = [
            validateAtmostOncePeriodInSeconds: (int)Integer.parseInt(params.pollInterval)/2,
        ]
        poolOptions += params.jdbcPoolProperties ?: [:]

        deployer.createPGConnectionPool(JDBC_POOL, JDBC_RESOURCE, params.dbUser, params.dbPassword, dbUrl, poolOptions)
    }

    def deleteJdbcResources() {
        deployer.deleteJdbcResource( JDBC_RESOURCE )
        deployer.deleteJdbcConnectionPool( JDBC_POOL )
    }

}
