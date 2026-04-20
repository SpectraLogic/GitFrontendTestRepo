pipeline {
  agent any
   
    
    
    stages {
        stage('Environment Info') {
            steps {
                sh '''
                    echo "Git LFS version:"
                    git lfs version
                    echo "Java version:"
                    java -version
                    echo "Docker version:"
                    docker --version
                '''
            }
        }
        
        stage('Checkout') {
            steps {
                // Jenkins handles checkout automatically, just pull LFS files
                sh 'git lfs pull'
            }
        }
        stage('Pre-build Debug') {
            steps {
                 dir('product/frontend') {
                    sh '''
                    echo "=== Environment Debug ==="
                    pwd
                    whoami
                    echo "DataPlanner structure:"
                    ls -la DataPlanner/ || echo "No DataPlanner directory"
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
                dir('product/frontend') {
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
              
                dir('product/frontend') {
                    // Start the PostgreSQL container
                    sh './gradlew test'
                }
            }
        }

        
    }
    
    post {
          success {
            dir('product/frontend/DataPlanner/build/') {
                echo "Copying distribution files..."
                // Use a single, clear copy command
                sh 'cp -r distributions/* /Users/ashapillai/JenkinsLibs/'
            }
            dir('product/frontend/common/build') {
                sh 'cp -r libs/* /Users/ashapillai/JenkinsLibs/'
            }
            // Add other copy commands as needed
            echo "Finished copying files."
        }
        
        failure {
           echo "Build failed. Not copying files."
        } 
    }
}
