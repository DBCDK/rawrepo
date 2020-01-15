def workerNode = "devel9"

pipeline {
    agent { label workerNode }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20', daysToKeepStr: '20'))
        disableConcurrentBuilds()
        timeout(time: 1, unit: 'HOURS')
        timestamps()
    }
    triggers {
        pollSCM('H/03 * * * *')
        upstream(upstreamProjects: "Docker-payara5-bump-trigger",
                threshold: hudson.model.Result.SUCCESS)
    }
    environment {
        MAVEN_OPTS = "-XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Dorg.slf4j.simpleLogger.showThreadName=true"
        JAVA_OPTS = "-XX:-UseSplitVerifier"
        PROJECT_VERSION = readMavenPom().getVersion()
        DOCKER_IMAGE_VERSION = "${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
        DOCKER_IMAGE_DIT_VERSION = "DIT-${env.BUILD_NUMBER}"
        GITLAB_PRIVATE_TOKEN = credentials("metascrum-gitlab-api-token")
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
                    def imageNamePostgres = "rawrepo-postgres-${PROJECT_VERSION}".toLowerCase()
                    def imageNameMaintain = "rawrepo-maintain-${PROJECT_VERSION}".toLowerCase()
                    def imageNameContentService = "rawrepo-content-service-${PROJECT_VERSION}".toLowerCase()

                    def dockerPostgres = docker.build("${repo}/${imageNamePostgres}:${DOCKER_IMAGE_VERSION}", '--pull --no-cache ./access')
                    def dockerMaintain = docker.build("${repo}/${imageNameMaintain}:${DOCKER_IMAGE_VERSION}", '--pull --no-cache ./maintain ')
                    def dockerContentService = docker.build("${repo}/${imageNameContentService}:${DOCKER_IMAGE_VERSION}", '--pull --no-cache ./content-service ')

                    dockerPostgres.push()
                    dockerMaintain.push()
                    dockerContentService.push()

                    if (env.BRANCH_NAME == 'master') {
                        dockerPostgres.push("${DOCKER_IMAGE_DIT_VERSION}")
                        dockerMaintain.push("${DOCKER_IMAGE_DIT_VERSION}")
                        dockerContentService.push("${DOCKER_IMAGE_DIT_VERSION}")
                    }
                }
            }
        }
        stage("Update DIT") {
            agent {
                docker {
                    label workerNode
                    image "docker.dbc.dk/build-env:latest"
                    alwaysPull true
                }
            }
            when {
                expression {
                    (currentBuild.result == null || currentBuild.result == 'SUCCESS') && env.BRANCH_NAME == 'master'
                }
            }
            steps {
                script {
                    dir("deploy") {
                        sh """
                            set-new-version services/rawrepo/ ${env.GITLAB_PRIVATE_TOKEN} metascrum/dit-gitops-secrets ${DOCKER_IMAGE_DIT_VERSION} -b master
                        """
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
