pipeline {
    agent { label 'devel8' }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20', daysToKeepStr: '20'))
        disableConcurrentBuilds()
        timeout(time: 1, unit: 'HOURS')
        timestamps()
    }
    triggers { pollSCM('H/3 * * * *') }
    
    environment {
        MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Dorg.slf4j.simpleLogger.showThreadName=true"
        JAVA_OPTS="-XX:-UseSplitVerifier"
        VERSION=readMavenPom().getVersion()
    }

    tools {
        maven 'maven 3.5'
    }

    stages {
        stage('Build') {
            steps {
                script {
                    withMaven( maven: 'maven 3.5', options: [
                            findbugsPublisher(disabled: true),
                            openTasksPublisher(highPriorityTaskIdentifiers: 'todo', ignoreCase: true, lowPriorityTaskIdentifiers: 'review', normalPriorityTaskIdentifiers: 'fixme,fix')
                    ]) {
                        sh "mvn clean"
                        sh "mvn install pmd:pmd findbugs:findbugs javadoc:aggregate -Dmaven.test.failure.ignore=false  -pl '!debian,!docker,!docker/introspect,!docker/content-service,!docker/maintain,!docker/postgres'"
                    }
                }
            }
            post {
                success {
                    pmd canComputeNew: false, defaultEncoding: '', healthy: '', pattern: '**/pmd.xml', unHealthy: ''
                    findbugs canComputeNew: false, defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', pattern: '', unHealthy: ''
                    archiveArtifacts 'access/schema/*.sql, access/schema/*.md5, **/target/*.jar, **/target/*.war, **/target/*.zip, **/target/*.md5, docs/RR-documentation.pdf'
                    warnings canComputeNew: false, canResolveRelativePaths: false, categoriesPattern: '', consoleParsers: [[parserName: 'Java Compiler (javac)']], defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', unHealthy: ''
                    sh "mvn -pl access deploy"
                }
            }
        }

        stage('Docker') {
            steps {
                script {
                    def allDockerFiles = findFiles(glob: '**/Dockerfile')
                    def dockerFiles = allDockerFiles.findAll { f -> !f.path.startsWith("docker") }

                    for (def f : dockerFiles) {
                        def dirName = f.path.take(f.path.length() - 11)
                        def projectName = f.path.substring(0, f.path.indexOf('/'))

                        dir(dirName) {
                            def imageName = "rawrepo-${projectName}-${VERSION}".toLowerCase()
                            def imageLabel = env.BUILD_NUMBER
                            if (env.BRANCH_NAME && !env.BRANCH_NAME ==~ /master|trunk/ ) {
                                println("Using branch_name ${env.BRANCH_NAME}")
                                imageLabel = env.BRANCH_NAME.split(/\//)[-1]
                                imageLabel = imageLabel.toLowerCase()
                            }

                            println("In ${dirName} build ${projectName} as ${imageName}:$imageLabel")
                            sh 'rm -f *.war ; cp  ../../../target/*.war .'
                            def app = docker.build("$imageName:${imageLabel}".toLowerCase(), '--pull --no-cache .')

                            if (currentBuild.resultIsBetterOrEqualTo('SUCCESS')) {
                                docker.withRegistry('https://docker-i.dbc.dk', 'docker') {
                                    app.push()
                                    if( env.BRANCH_NAME ==~ /master|trunk/ ) {
                                        app.push "latest"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('dpkg') {
            when {
                expression { env.BRANCH_NAME ==~ /master|trunk/ }
            }
            steps {
                sh "mvn -pl debian install"
            }
        }
    }

}
