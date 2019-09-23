pipeline {
  agent any
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
        sh 'npm ci'
      }
    }
    stage('Coding Standards') {
      steps {
        sh 'npm run lint'
      }
    }
    stage('Build') {
      steps {
        sh 'npm run build'
      }
    }
    stage('Publish') {
      when {
        buildingTag()
      }
      steps {
        sh 'npm publish'
      }
    }
  }
}
