pipeline {
    agent { label 'devel8' }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20', daysToKeepStr: '20'))
        disableConcurrentBuilds()
        timeout(time: 1, unit: 'HOURS')
        timestamps()
    }
    triggers { pollSCM('H/03 * * * *') }

    environment {
        MAVEN_OPTS = "-XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Dorg.slf4j.simpleLogger.showThreadName=true"
        JAVA_OPTS = "-XX:-UseSplitVerifier"
        DOCKER_IMAGE_VERSION = "${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
        DOCKER_IMAGE_DIT_VERSION = "DIT-${env.BUILD_NUMBER}"
    }

    tools {
        maven 'maven 3.5'
    }

    stages {
        stage("Clean Workspace") {
            steps {
                deleteDir()
                checkout scm
            }
        }

        stage('Build') {
            steps {
                script {
                    withMaven(maven: 'maven 3.5', options: [
                            findbugsPublisher(disabled: true),
                            openTasksPublisher(highPriorityTaskIdentifiers: 'todo', ignoreCase: true, lowPriorityTaskIdentifiers: 'review', normalPriorityTaskIdentifiers: 'fixme,fix')
                    ]) {
                        sh "mvn verify pmd:pmd findbugs:findbugs javadoc:aggregate -Dmaven.test.failure.ignore=false  -pl '!debian'"
                    }
                }
            }
            post {
                success {
                    pmd canComputeNew: false, defaultEncoding: '', healthy: '', pattern: '**/pmd.xml', unHealthy: ''
                    findbugs canComputeNew: false, defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', pattern: '', unHealthy: ''
                    archiveArtifacts 'access/schema/*.sql, access/schema/*.md5, **/target/*.jar, **/target/*.war, **/target/*.zip, **/target/*.md5, docs/RR-documentation.pdf'
                    warnings canComputeNew: false, canResolveRelativePaths: false, categoriesPattern: '', consoleParsers: [[parserName: 'Java Compiler (javac)']], defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', unHealthy: ''
                }
            }
        }

        stage('Docker') {
            when {
                expression {
                    currentBuild.result == null || currentBuild.result == 'SUCCESS'
                }
            }
            steps {
                script {
                    def repo = "docker-io.dbc.dk"
                    def dockerPostgres = docker.build("${repo}/rawrepo-postgres:${DOCKER_IMAGE_VERSION}")
                    def dockerIntrospect = docker.build("${repo}/rawrepo-introspect:${DOCKER_IMAGE_VERSION}")

                    dockerPostgres.push()
                    dockerIntrospect.push()

                    if (env.BRANCH_NAME == 'master') {
                        dockerPostgres.push("${DOCKER_IMAGE_DIT_VERSION}")
                        dockerIntrospect.push("${DOCKER_IMAGE_DIT_VERSION}")
                    }
                }
            }
        }

        stage('dpkg') {
            when {
                expression { env.BRANCH_NAME == 'master' }
            }
            steps {
                sh "mvn -pl debian deploy"
                sh "mvn -pl access deploy"
            }
        }
    }

}
