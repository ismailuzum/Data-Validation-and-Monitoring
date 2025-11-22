pipeline {
    agent any

    environment {
        SLACK_WEBHOOK_URL = credentials('slack-webhook-url')
    }

    stages {

        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/ismailuzum/Data-Validation-and-Monitoring.git'
            }
        }

        stage('Install Python Packages') {
            steps {
                sh 'python3 --version'
                sh 'pip install -r requirements.txt'
            }
        }

        stage('Run Data Validation Pipeline') {
            steps {
                sh 'python dq_pipeline_final.py'
            }
        }

        stage('Archive Failed Rows') {
            when {
                expression { fileExists('failed_rows.csv') }
            }
            steps {
                archiveArtifacts artifacts: 'failed_rows.csv'
            }
        }
    }

    post {
        success {
            echo "Pipeline SUCCEEDED!"
        }
        failure {
            echo "Pipeline FAILED!"
        }
    }
}
