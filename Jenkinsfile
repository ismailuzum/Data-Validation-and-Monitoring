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

        stage('Install Dependencies') {
            steps {
                sh 'pip3 install --upgrade pip'
                sh 'pip3 install -r requirements.txt'
            }
        }

        stage('Run Data Validation Pipeline') {
            steps {
                sh 'python3 dq_pipeline_final.py'
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
