/*
 * This file is part of opensearch.
 * Copyright (c) 2013, Dansk Bibliotekscenter a/s,
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

import dk.dbc.glu.utils.xml.XmlUpdater
import dk.dbc.glu.utils.glassfish.GlassFishAppDeployer

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.linkedin.glu.agent.api.MountPoint
import org.linkedin.glu.agent.api.ScriptFailedException
import org.linkedin.glu.agent.api.Shell
import org.linkedin.glu.agent.impl.capabilities.ShellImpl
import org.linkedin.groovy.util.io.fs.FileSystemImpl
import org.linkedin.util.io.resource.Resource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import static javax.xml.xpath.XPathConstants.STRING
import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertEquals

public class IndexerDeployScriptTest {
    private final static MOUNT_POINT_NAME = 'unittest'
    private final static ARTIFACT_NAME = "rawrepo-solr-indexer.war"
    private final static ARTIFACT = IndexerDeployScriptTest.getResource( "/" + ARTIFACT_NAME )
    private final static CONTEXT = "/rawrepoindexer"
    private final static SOLR_URL = "http://localhost:12345/solr"
    private final static DB_URL = "jdbc:postgresql://localhost:12345/db"
    private final static DB_USER = "user"
    private final static DB_PASSWORD = "password"
    private final static DB_WORKER = "worker"
    private final static POLL_INTERVAL = "5"
    private final static THREAD_POOL_SIZE = "2"
    private static LOG_FOLDER = "log-files"


    class GluScript extends IndexerDeployScript {
        Logger log = LoggerFactory.getLogger( GluScript.class )
        MountPoint mountPoint = MountPoint.fromPath( MOUNT_POINT_NAME )

        Shell shell
        Map params

        GluScript( Shell shell, Map initParameters=[:] ) {
            this.shell = shell
            this.params = initParameters
        }
    }

    private static tmpFileSystem
    private Shell shellImpl
    Resource mountPointFolder

    @BeforeClass
    static void setUpClass() {
        dk.dbc.glu.utils.ExceptionJdk17Workaround.installWorkaround()
        tmpFileSystem = FileSystemImpl.createTempFileSystem()
    }

    @AfterClass
    static void tearDownClass() {
        tmpFileSystem.destroy()
    }

    @Before
    void setUp() {
        tmpFileSystem.root.file.mkdir()
        shellImpl = new ShellImpl( fileSystem: tmpFileSystem )
        mountPointFolder = tmpFileSystem.root."$MOUNT_POINT_NAME"
    }

    @After
    void tearDown() {
        shellImpl.rmdirs( tmpFileSystem.root )
        shellImpl = null
    }

    @Test
    void install_mountPointIsSet_createsMountPointFolderRelativeToFileSystemRoot() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()

        assertThat( "Mount point is created", tmpFileSystem.root."$MOUNT_POINT_NAME".file.exists(), is( true ) )

        // also assert that folder is exposed
        assertThat( tmpFileSystem.root."$MOUNT_POINT_NAME", is( instance.rootFolder ) )
    }


    @Test( expected=NullPointerException )
    void install_artifactInitParameterIsNotSet_throwsNullPointerException() {
        def instance = new GluScript( shellImpl, [ solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=NullPointerException )
    void install_artifactInitParameterIsNull_throwsNullPointerException() {
        def instance = new GluScript( shellImpl, [ artifact: null, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=IllegalArgumentException )
    void install_artifactInitParameterIsEmpty_throwsIllegalArgumentException() {
        def instance = new GluScript( shellImpl, [ artifact: "", solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=FileNotFoundException )
    void install_artifactInitParameterPointsToNonExistingResource_throwsFileNotFoundException() {
        def instance = new GluScript( shellImpl, [ artifact: "no-such-file", solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_solrUrlInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_solrUrlInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: null,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbUrlInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbUrlInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: null, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbUserInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbUserInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: null, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbPasswordInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbPasswordInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: null, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbWorkerInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_dbWorkerInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: null, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_pollIntervalParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_pollIntervalInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: null, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_threadPoolSizeInitParameterIsNotSet_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL ] )
        instance.install()
    }

    @Test( expected=ScriptFailedException )
    void install_threadPoolSizeInitParameterIsNull_throwsScriptFailedException() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: null ] )
        instance.install()
    }

    @Test
    void install_glassfishPropertiesInitParameterNotSet_usesDefaultGlassFishProperties() {
        def defaultProperties = [
            port: '8080',
            username: 'admin',
            password: 'admin',
        ]

        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()

        ['port', 'username', 'password'].each() {
          assertThat( instance.glassfishProperties[it], notNullValue() )
          assertThat( instance.glassfishProperties[it], is( defaultProperties[it] ) )
        }

        assertThat(instance.glassfishProperties.baseUrl as String, is("https://localhost:8080"))
    }

    @Test
    void install_glassfishPropertiesInitParameterIsSet_overridesDefaultGlassFishProperties() {
        def customProperties = [
            port: '4242',
            username: 'user',
            password: 'pass',
            insecure: true,
        ]

        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE,
            glassfishProperties: customProperties] )
        instance.install()

        ['port', 'username', 'password'].each() {
          assertThat( instance.glassfishProperties[it], notNullValue() )
          assertThat( instance.glassfishProperties[it], is( customProperties[it] ) )
        }

        assertThat(instance.glassfishProperties.baseUrl as String, is("http://localhost:4242"))
    }

    @Test
    void configure_contextPathInitIsNotSet_usesDefaultContextPath() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()

        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                assertThat(appProperties.name, is(instance.appName))
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
        instance.configure()

        assertThat( instance.contextPath, is( CONTEXT as String ) )
    }

    @Test
    void configure_contextPathInitIsSet_usesContextPath() {
        String customPath = "context"
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE,
            contextPath: customPath] )
        instance.install()
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                assertThat(appProperties.name, is(instance.appName))
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
        instance.configure()

        assertThat( instance.contextPath, is( customPath ) )
    }

    @Test
    void configure_addsLoggingProperties() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                assertThat(appProperties.name, is(instance.appName))
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
        instance.configure()

        assertThat( instance.getRootFolder().staging."WEB-INF".classes."logback.xml".exists() as Boolean, is( true ) )
    }

    @Test
    void configure_whenInstalled_exposesLogFiles() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
        instance.configure()

        def expected = [
            "indexer",
            "indexer-error",
        ]

        expected.each { obj ->
            assertThat( mountPointFolder."$LOG_FOLDER"."${obj}.log".file.getPath(), is( instance.logs."$obj" ) )
        }
    }

    @Test
    void configure_whenInstalled_configuresEjbXml() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
        instance.configure()

        def file = instance.getRootFolder().staging."WEB-INF"."ejb-jar.xml"
        assertThat( file.exists() as Boolean, is( true ) )
        def content = shellImpl.readContent( file )

        def ejbXml = new XmlUpdater( content, [ ns: 'http://xmlns.jcp.org/xml/ns/javaee' ] )

        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[1]/ns:env-entry[1]/ns:env-entry-name", STRING ) as String, is( "solrUrl" ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[1]/ns:env-entry[1]/ns:env-entry-value", STRING ) as String, is( SOLR_URL ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[1]/ns:env-entry[2]/ns:env-entry-name", STRING ) as String, is( "workerName" ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[1]/ns:env-entry[2]/ns:env-entry-value", STRING ) as String, is( DB_WORKER ) )

        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[2]/ns:env-entry[1]/ns:env-entry-name", STRING ) as String, is( "timeout" ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[2]/ns:env-entry[1]/ns:env-entry-value", STRING ) as String, is( POLL_INTERVAL ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[2]/ns:env-entry[2]/ns:env-entry-name", STRING ) as String, is( "maxConcurrent" ) )
        assertThat( ejbXml.evaluateXpath( "/ns:ejb-jar/ns:enterprise-beans/ns:session[2]/ns:env-entry[2]/ns:env-entry-value", STRING ) as String, is( THREAD_POOL_SIZE ) )
    }

    @Test
    void configure_whenInstalled_configuresConnectionPool() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            boolean createJdbcConnectionPoolCalled = false
            boolean createJdbcResourceCalled = false
            Object deploy(Map<String, String> appProperties) {
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                createJdbcConnectionPoolCalled = true
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                assertThat( "Pool was created before resource", instance.deployer.createJdbcConnectionPoolCalled, is( true ) )
                createJdbcResourceCalled = true
                return new Object()
            }
        }
        instance.configure()
        assertThat( "createJdbcConnectionPool was called", instance.deployer.createJdbcConnectionPoolCalled, is( true ) )
        assertThat( "createJdbcResource was called", instance.deployer.createJdbcResourceCalled, is( true ) )
    }

    @Test
    void start_webappCanBeDeployedAndStarted_issuesGlassfishStartCommand() {

        def instance = new GluScript( shellImpl, [:] )
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
          Object start(String appName) {
            assertThat( instance.appName, is( appName ) )
          }
        }
        instance.start()
    }

    @Test
    void stop_webappCanBeStoppedAndUndeployed_issuesGlassfishStopCommand() {

        def instance = new GluScript( shellImpl, [:] )
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
          Object stop(String appName) {
            assertThat( instance.appName, is( appName ) )
          }
        }
        instance.stop()
    }

    @Test
    void unconfigure_deletesConnectionPool() {
        def instance = new GluScript( shellImpl, [:] )
        instance.deployer = new GlassFishAppDeployer(['baseUrl':'url']) {
            boolean deleteJdbcConnectionPoolCalled = false
            boolean deleteJdbcResourceCalled = false
            Object undeploy(String appName) {
                return new Object()
            }
            Object deleteJdbcResource(String id) {
                deleteJdbcResourceCalled = true
                return new Object()
            }
            Object deleteJdbcConnectionPool(String name) {
                assertThat( "JDBC Resource must be deleted before deleting pool", instance.deployer.deleteJdbcResourceCalled, is( true ) )
                deleteJdbcConnectionPoolCalled = true
                return new Object()
            }
        }
        instance.unconfigure()
        assertThat( "deleteJdbcResource was called", instance.deployer.deleteJdbcResourceCalled, is( true ) )
        assertThat( "deleteJdbcConnectionPool was called", instance.deployer.deleteJdbcConnectionPoolCalled, is( true ) )
    }



    @Test
    void uninstall_deletesMountPointFolder() {
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE ] )
        instance.install()
        instance.uninstall()

        assertThat( "Mount point is deleted", tmpFileSystem.root."$MOUNT_POINT_NAME".file.exists() as Boolean, is( false ) )
    }

    @Test
    void configure_whenInstalledWithCustomLogDirectory_exposesLogFolder() {
        def logDir = tmpFileSystem.root."custom-log-files";
        def logDirPath = logDir.file.getPath();
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE,
            logDir: logDirPath ] )
        instance.install()
        instance.deployer = mockGlassFishAppDeployer();
        instance.configure()

        assertEquals( logDirPath, instance.logsDir )
    }

    @Test
    void configure_whenInstalledWithCustomLogDirectory_exposesLogFiles() {
        def logDir = tmpFileSystem.root."custom-log-files";
        def logDirPath = logDir.file.getPath();
        def instance = new GluScript( shellImpl, [ artifact: ARTIFACT, solrUrl: SOLR_URL,
            dbUrl: DB_URL, dbUser: DB_USER, dbPassword: DB_PASSWORD, dbQueueWorker: DB_WORKER, pollInterval: POLL_INTERVAL, threadPoolSize: THREAD_POOL_SIZE,
            logDir: logDirPath ] )
        instance.install()
        instance.deployer = mockGlassFishAppDeployer();
        instance.configure()

        def expected = [
            "indexer",
            "indexer-error",
        ]

        expected.each { obj ->
            assertEquals( logDir."${obj}.log".file.getPath(), instance.logs."$obj" )
        }
    }

    GlassFishAppDeployer mockGlassFishAppDeployer(){
        return new GlassFishAppDeployer(['baseUrl':'url']) {
            Object deploy(Map<String, String> appProperties) {
                return new Object()
            }
            Object createJdbcConnectionPool(Map<String, String> args) {
                return new Object()
            }
            Object createJdbcResource(Map<String, String> args) {
                return new Object()
            }
        }
    }
}
