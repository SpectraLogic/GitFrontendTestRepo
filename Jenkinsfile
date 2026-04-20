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
        PG_CONTAINER = "pg-${env.JOB_NAME}-${env.BUILD_NUMBER}".replaceAll('[^A-Za-z0-9_.-]', '-')
        PG_DATA_DIR = "/tmp/pgdata-${env.BUILD_NUMBER}"
        // The public-cloud tests (Azure/AWS) look for containers named exactly
        // "azurite" and "localstack" on localhost. disableConcurrentBuilds() on
        // this pipeline prevents collisions across runs.
        AZURITE_CONTAINER = 'azurite'
        LOCALSTACK_CONTAINER = 'localstack'
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

        stage('Start Database') {
            steps {
                sh '''
                    # Remove any leftover container/data from a prior failed run
                    docker rm -f "${PG_CONTAINER}" 2>/dev/null || true
                    sudo rm -rf "${PG_DATA_DIR}" 2>/dev/null || rm -rf "${PG_DATA_DIR}"
                    mkdir -p "${PG_DATA_DIR}"

                    # Bind-mount the data dir using the same path on host and container
                    # so that SHOW data_directory returns a path that also exists on the
                    # Jenkins host (where the test JVM runs).
                    docker run -d \
                        --name "${PG_CONTAINER}" \
                        -e POSTGRES_USER=Administrator \
                        -e POSTGRES_PASSWORD= \
                        -e POSTGRES_HOST_AUTH_METHOD=trust \
                        -e POSTGRES_INITDB_ARGS=--lc-collate=C \
                        -e PGDATA="${PG_DATA_DIR}" \
                        -v "${PG_DATA_DIR}:${PG_DATA_DIR}" \
                        -p 5432:5432 \
                        postgres:18

                    echo "Waiting for Postgres to accept connections..."
                    for i in $(seq 1 30); do
                        if docker exec "${PG_CONTAINER}" pg_isready -U Administrator >/dev/null 2>&1; then
                            echo "Postgres is ready."
                            exit 0
                        fi
                        sleep 2
                    done
                    echo "Postgres failed to become ready in time."
                    docker logs "${PG_CONTAINER}" || true
                    exit 1
                '''
            }
        }

        stage('Start Public Cloud Emulators') {
            steps {
                sh '''
                    # Remove any leftover containers from a prior failed run.
                    docker rm -fv "${AZURITE_CONTAINER}" 2>/dev/null || true
                    docker rm -fv "${LOCALSTACK_CONTAINER}" 2>/dev/null || true

                    # Azurite — mirrors docker/docker-compose-azurite.yml.
                    docker run -d \
                        --name "${AZURITE_CONTAINER}" \
                        -e AZURITE_ACCOUNTS='devstoreaccount1:Ss0sk4dZsuH0Cji92F1Ye2kuoEhv+mmYCLfLzGrdw0A1zQagbiBBbnHJNiALudX5nXXZkc4lxT0nFREbg8lpAQ==' \
                        -p 10000:10000 -p 10001:10001 -p 10002:10002 \
                        mcr.microsoft.com/azure-storage/azurite

                    # LocalStack — mirrors docker/docker-compose-localstack.yml.
                    docker run -d \
                        --name "${LOCALSTACK_CONTAINER}" \
                        -e SERVICES=s3 \
                        -e DEFAULT_REGION=us-east-1 \
                        -e HOSTNAME_EXTERNAL=localhost \
                        -e HOSTNAMES_ENABLE_VIRTUAL_HOSTS=true \
                        -e DEBUG=1 \
                        -e AWS_ACCESS_KEY_ID=test \
                        -e AWS_SECRET_ACCESS_KEY=test \
                        -e LOCALSTACK_ACKNOWLEDGE_ACCOUNT_REQUIREMENT=1 \
                        -p 4566:4566 \
                        -p 127.0.0.1:4510-4559:4510-4559 \
                        -v /var/run/docker.sock:/var/run/docker.sock \
                        localstack/localstack:3.4.0

                    echo "Waiting for Azurite (blob endpoint on :10000)..."
                    for i in $(seq 1 30); do
                        if curl -fsS -o /dev/null -w '%{http_code}' http://localhost:10000/devstoreaccount1 2>/dev/null | grep -qE '^(200|400|403)$'; then
                            echo "Azurite is ready."
                            break
                        fi
                        sleep 2
                    done

                    echo "Waiting for LocalStack (health endpoint on :4566)..."
                    for i in $(seq 1 30); do
                        if curl -fsS http://localhost:4566/_localstack/health 2>/dev/null | grep -q '"s3"'; then
                            echo "LocalStack is ready."
                            break
                        fi
                        sleep 2
                    done
                '''
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

        always {
            sh '''
                # Stop and remove the container AND any anonymous volumes it
                # created. The postgres image declares VOLUME /var/lib/postgresql/data,
                # so an anonymous volume is created each run even though our actual
                # data lives in the bind mount. The -v flag on docker rm reaps it.
                docker rm -fv "${PG_CONTAINER}" 2>/dev/null || true
                docker rm -fv "${AZURITE_CONTAINER}" 2>/dev/null || true
                docker rm -fv "${LOCALSTACK_CONTAINER}" 2>/dev/null || true

                # Clean up the bind-mounted data directory on the host.
                sudo rm -rf "${PG_DATA_DIR}" 2>/dev/null || rm -rf "${PG_DATA_DIR}" 2>/dev/null || true
            '''
        }
    }
}