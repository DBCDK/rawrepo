pipeline {
    agent { label 'devel8' }
    options {
        buildDiscarder(logRotator(numToKeepStr: '20'))
        disableConcurrentBuilds()
        timeout(time: 1, unit: 'HOURS') }
        options { timestamps() }
    }

    stages {
        stage('checkout ') {
            checkout scm
        }

        stage('mvn build') {
            steps {
                sh "mvn install pmd:pmd findbugs:findbugs javadoc:aggregate -Dmaven.test.failure.ignore=false -Djetty.port=${JETTY_PORT} -pl '!debian'"
            }
        }

        stage('build Debian package') {
            steps {
                sh "svn upgrade && mvn -pl debian install"
            }
        }
    }
    post {
        always {
            pmd canComputeNew: false, defaultEncoding: '', healthy: '', pattern: '**/pmd.xml', unHealthy: ''
            junit '**/target/*-reports/*.xml'
            openTasks canComputeNew: false, defaultEncoding: '', excludePattern: '', healthy: '', high: 'todo', ignoreCase: true, low: 'review', normal: 'fixme,fix', pattern: '', unHealthy: ''
            findbugs canComputeNew: false, defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', pattern: '', unHealthy: ''
            warnings canComputeNew: false, canResolveRelativePaths: false, categoriesPattern: '', consoleParsers: [[parserName: 'Java Compiler (javac)']], defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', unHealthy: ''
            archiveArtifacts 'access/schema/*.sql, access/schema/*.md5, **/target/*.jar, **/target/*.war, **/target/*.zip, **/target/*.md5, docs/RR-documentation.pdf'
        }
        
    }


}