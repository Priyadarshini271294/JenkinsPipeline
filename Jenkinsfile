pipeline {
  agent any
  environment {
    PHANTOMJS_PLATFORM = 'linux'
  }
  options {
    timestamps()
    buildDiscarder(
      logRotator(
        daysToKeepStr: '21',
        numToKeepStr: '10',
        artifactDaysToKeepStr: '21',
        artifactNumToKeepStr: '10'
      )
    )
  }
  stages {
    stage('Initialize') {
      steps {
        // Start tracking concurrent build order.
        milestone 1
        // Ensure the resource-heavy 'phing initialize' step is serialized.
        // Ensure *newer* builds are pulled off the queue first.
        lock(resource: 'JenkinsSample:initialize', inversePrecedence: true) {
          sh '''
            phing \
                -verbose \
                initialize
          '''
          // Discard any older builds.
          milestone 2
        }
      }
    }
    stage('Coding Standards') {
      steps {
        sh '''
          phing \
              -verbose \
              -Dprepare.done=true \
              -Dcopy-environment.done=true \
              -Dinstall-php-dependencies.done=true \
              -Dinstall-js-dependencies.done=true \
              coding-standards
        '''
      }
      post {
        always {
          checkstyle canComputeNew: false,
              canRunOnFailed: true,
              defaultEncoding: '',
              healthy: '',
              pattern: 'build/reports/lint/*.xml',
              unHealthy: ''
        }
      }
    }
    stage('Unit Test') {
      // Only unit test "deployable" branches or branches merging into "deployable" branches.
      when {
        anyOf {
          changeRequest target: 'develop'
          changeRequest target: 'release/.*', comparator: 'REGEXP'
          changeRequest target: 'master'
          changeRequest target: 'hotfix/.*', comparator: 'REGEXP'
          branch 'develop'
          branch 'release/*'
          branch 'hotfix/*'
          tag pattern: '[0-9]+\\.[0-9]+', comparator: 'REGEXP'
        }
      }
      steps {
        sh '''
          phing \
              -verbose \
              -Dprepare.done=true \
              -Dcopy-environment.done=true \
              -Dinstall-php-dependencies.done=true \
              -Dinstall-js-dependencies.done=true \
              unit-test
        '''
      }
      post {
        always {
          junit 'build/reports/test-unit/*.xml'
        }
      }
    }
    stage('Package') {
      // Only package "deployable" branches, including PRs that have the 'package' label
      when {
        anyOf {
          branch 'develop'
          branch 'release/*'
          branch 'hotfix/*'
          tag pattern: '[0-9]+\\.[0-9]+', comparator: 'REGEXP'
          expression { return changeRequest() && pullRequest.labels.any { it == 'package' } }
        }
      }
      steps {
        sh '''
          phing \
              -verbose \
              -Dprepare.done=true \
              -Dcopy-environment.done=true \
              -Dinstall-php-dependencies.done=true \
              -Dinstall-js-dependencies.done=true \
              package
        '''
      }
      post {
        success {
          // Archive for records.
          archiveArtifacts artifacts: 'dist/*', fingerprint: true
        }
      }
    }
    stage('Deploy Staging') {
      when {
        anyOf {
          branch 'develop'
          branch 'release/*'
          branch 'hotfix/*'
        }
      }
      steps {
        script {
          env.ERWIN_JenkinsSample_STAGING_ENVIRONMENT_NAME = getEnvironmentName()
          def buildVersion = getBuildVersion()
          def dbIdentifier = getDbIdentifier()
          def inventory = getInventory()
          // Ensure only one build can deploy to the staging server at once.
          // Ensure *newer* builds are pulled off the queue first.
          lock(resource: "JenkinsSample:server:${env.ERWIN_JenkinsSample_STAGING_ENVIRONMENT_NAME}.myerwin.io", inversePrecedence: true) {
            build job: 'JenkinsSample-deploy', parameters: [
              string(name: 'ENVIRONMENT_NAME', value: String.valueOf(env.ERWIN_JenkinsSample_STAGING_ENVIRONMENT_NAME)),
              string(name: 'BUILD_VERSION', value: String.valueOf(buildVersion)),
              string(name: 'DB_IDENTIFIER', value: String.valueOf(dbIdentifier)),
              string(name: 'INVENTORY', value: String.valueOf(inventory)),
              string(name: 'PACKAGE_PATH', value: "${env.WORKSPACE}/dist/JenkinsSample.tgz")
            ]
            // Discard any older builds.
            milestone 5
          }
        }
      }
    }
    stage('API Test') {
      when {
        anyOf {
          branch 'develop'
          branch 'release/*'
          branch 'hotfix/*'
        }
      }
      steps {
        // Ensure the resource-heavy 'npm run test-api:ci' step is serialized.
        // Ensure *newer* builds are pulled off the queue first.
        lock(resource: "JenkinsSample:server:${env.ERWIN_JenkinsSample_STAGING_ENVIRONMENT_NAME}.myerwin.io", inversePrecedence: true) {
          sshagent(['3819723a-5a38-4d67-bffc-e5f9d0e4f5b2']) {
            sh 'jenkins/run-api-tests.sh ${ERWIN_JenkinsSample_STAGING_ENVIRONMENT_NAME}.myerwin.io'
          }
          milestone 6
        }
      }
      post {
        always {
          junit 'build/reports/test-api/mocha.xml'
        }
      }
    }
    stage('Publish') {
      when {
        anyOf {
          branch 'develop'
          branch 'release/*'
          branch 'hotfix/*'
          tag pattern: '[0-9]+\\.[0-9]+', comparator: 'REGEXP'
          expression { return changeRequest() && pullRequest.labels.any { it == 'package' } }
        }
      }
      steps {
        script {
          def buildType = getBuildType()
          def nexusRepository = getNexusRepository(buildType)
          def nexusVersion = getNexusVersion(buildType)
          nexusArtifactUploader (
            nexusVersion: 'nexus3',
            protocol: 'https',
            nexusUrl: 'nexus.myerwin.io',
            groupId: 'com.erwin',
            version: "${nexusVersion}",
            repository: "${nexusRepository}",
            credentialsId: 'nexus-publisher',
            artifacts: [
              [
                artifactId: 'erwin-JenkinsSample',
                classifier: '',
                file: './dist/JenkinsSample.tgz',
                type: 'tgz'
              ]
            ]
          )
        }
      }
    }
  }
  post {
    always {
      zulipNotification (
        stream: 'JenkinsSample',
        topic: "${env.JOB_NAME}"
      )
    }
  }
}

// Function definitions - Start

def getEnvironmentName() {
  switch (env.BRANCH_NAME) {
    case 'develop':
      return 'stagingbeta'
    case ~/release\/.*/:
      return 'stagingus'
    case ~/hotfix\/.*/:
      return 'testus'
    default:
      error 'Invalid branch for an environment name'
  }
}

def getBuildVersion() {
  def branchType = env.BRANCH_NAME.split('/')[0]
  switch (branchType) {
    case 'develop':
      return "beta-${env.BUILD_NUMBER}"
    case 'release':
    case 'hotfix':
      def branchVersion = env.BRANCH_NAME.split('/')[1]
      return "${branchVersion}-${env.BUILD_NUMBER}"
    default:
      error 'Invalid branch for a build version'
  }
}

def getDbIdentifier() {
  switch (env.BRANCH_NAME) {
    case 'develop':
      return 'JenkinsSample-erwin-stagingbeta-app'
    case ~/release\/.*/:
      return 'JenkinsSample-erwin-stagingus-app'
    case ~/hotfix\/.*/:
      return 'JenkinsSample-erwin-testus-app'
    default:
      error 'Invalid branch for a DB host'
  }
}

def getInventory() {
  switch (env.BRANCH_NAME) {
    case 'develop':
      return 'beta'
    case ~/release\/.*/:
    case ~/hotfix\/.*/:
      return 'us'
    default:
      error 'Invalid branch for an inventory'
  }
}

def getBuildType() {
  if (env.TAG_NAME) {
    return 'tag'
  }
  if (env.CHANGE_ID) {
    return 'pr'
  }
  return 'branch'
}

def getNexusRepository(type) {
  switch (type) {
    case 'tag':
      return 'maven-releases'
    default:
      return 'maven-snapshots'
  }
}

def getBranchVersion() {
  def branchType = env.BRANCH_NAME.split('/')[0]
  switch (branchType) {
    case 'develop':
      return 'beta-SNAPSHOT'
    case 'release':
    case 'hotfix':
      def branchVersion = env.BRANCH_NAME.split('/')[1]
      return "${branchVersion}-SNAPSHOT"
    default:
      error 'Invalid branch to publish to Nexus'
  }
}

def getNexusVersion(type) {
  switch (type) {
    case 'pr':
      return "pr${pullRequest.number}-SNAPSHOT"
    case 'branch':
      return getBranchVersion()
    case 'tag':
      return "${env.TAG_NAME}"
    default:
      error 'Invalid type to publish to Nexus'
  }
}

// Function definitions - End
