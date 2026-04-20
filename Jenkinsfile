def defineBuildTarget() {
    return 'linux'
}
pipeline {
    agent { label "LINUX" }

    parameters {
        string(defaultValue: '/scratch/packages/dataplanner',
               description: 'Path to archive artifacts, branch added automatically',
               name: 'BUILD_OUT_ROOT')
    }

    options {
        buildDiscarder(logRotator(daysToKeepStr: '30', artifactNumToKeepStr: '7'))
        disableConcurrentBuilds()
    }

    environment {
        // Set to '.' because your repo starts at the frontend folder level
        PROJECT_ROOT = '.'
        BUILD_TARGET = defineBuildTarget()
    }

    stages {
        stage('Environment Info') {
            steps {
                sh '''
                    echo "Java version:"
                    java -version
                    echo "Docker version:"
                    docker --version || echo "Docker not installed"
                '''
            }
        }
        stage('Pre-build Debug') {
            steps {
                 dir('.') {
                    sh '''
                    echo "=== Environment Debug ==="
                    pwd
                    whoami
                    echo "DataPlanner structure:"
                    ls -la
                    echo "Build directory before:"
                    ls -la DataPlanner/build/ || echo "No build directory yet"

                  echo "=== Gradle Configuration Check ==="
                    grep -n "application" DataPlanner/build.gradle || echo "No application config found"
                    grep -n "mainClass" DataPlanner/build.gradle || echo "No mainClass config found"
                    grep -n "distribution" DataPlanner/build.gradle || echo "No distribution config found"
                '''
                 }

            }
        }


        stage('Build') {
            steps {
                dir("${env.PROJECT_ROOT}") {
                    sh '''
                        # Force clean environment
                        unset JAVA_TOOL_OPTIONS || true
                        unset _JAVA_OPTIONS || true

                        # Verify Java setup
                        echo "Using Java version:"
                        java -version
                        echo "JAVA_HOME: $JAVA_HOME"
                        echo "PATH: $PATH"

                        # Make gradlew executable (in case it's not)
                        chmod +x gradlew

                        # Check Gradle version
                        ./gradlew --version
                        chmod +x packageAll.sh
                        # Run build with no daemon and fresh caches
                        ./packageAll.sh

                        echo "Test distribution build..."
                         # Ensure proper directory structure
                        #mkdir -p DataPlanner/src/main/dist
                        #mkdir -p DataPlanner/build/distributions
                        #./gradlew clean :DataPlanner:distTar --no-daemon --info
                         # Verify output
                    ls -la DataPlanner/build/distributions/ || echo "No distributions created"
                    '''
                }
            }
        }

        stage('Test') {

           steps {
            dir("${env.PROJECT_ROOT}") {
                    sh '''
                    ./testAll.sh
                    '''
                }

            }
        }


    }

    post {
        success {
            script {
                def branch = env.BRANCH_NAME ?: 'main'
                def outDir = "${params.BUILD_OUT_ROOT}/${branch}"
                sh "mkdir -pm 777 ${outDir} || true"
                sh "cp -r ${env.PROJECT_ROOT}/DataPlanner/build/distributions/* ${outDir}/"
                sh "cp -r ${env.PROJECT_ROOT}/common/build/libs/* ${outDir}/"
                echo "Copied artifacts to ${outDir}"
            }
            archiveArtifacts artifacts: 'DataPlanner/build/distributions/*, common/build/libs/*',
                             fingerprint: true,
                             allowEmptyArchive: false
        }

        failure {
            echo "Build failed. Not copying files."
        }
    }
}