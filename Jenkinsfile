pipeline {
  agent any

  environment {
    CLUSTER_NAME = "qoe"
    NAMESPACE    = "qoe"

    BACKEND_IMAGE  = "qoe/dashboard-backend:dev"
    FRONTEND_IMAGE = "qoe/dashboard-frontend:dev"
  }

  options {
    timestamps()
  }

  stages {

    stage('Checkout') {
        steps {
            git branch: 'main',
            url: 'https://github.com/allaliamine/QoE-Analysis-Platform.git'
        }
    }


    stage('Install Test Dependencies') {
      agent {
        docker {
          image 'python:3.11-slim'
          args '-u root:root'
        }
      }
      steps {
        sh '''
          pip install --upgrade pip
          pip install -r requirements.txt
          pip install pytest
        '''
      }
    }

    stage('Run Tests') {
      agent {
        docker {
          image 'python:3.11-slim'
          args '-u root:root'
        }
      }
      steps {
        sh '''
          pip install --upgrade pip
          pip install -r requirements.txt
          pip install pytest
          pytest tests/
        '''
      }
    }

    stage('Build Docker Images') {
      steps {
        sh '''
          docker build -t $BACKEND_IMAGE ./app/backend
          docker build -t $FRONTEND_IMAGE ./app/frontend
        '''
      }
    }

    stage('Load Images into kind') {
      steps {
        sh '''
          kind load docker-image $BACKEND_IMAGE --name $CLUSTER_NAME
          kind load docker-image $FRONTEND_IMAGE --name $CLUSTER_NAME
        '''
      }
    }

    stage('Deploy to Kubernetes') {
      steps {
        sh '''
          kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

          kubectl apply -f k8s/app/backend.yaml
          kubectl apply -f k8s/app/frontend.yaml

          kubectl rollout restart deployment/backend -n $NAMESPACE
          kubectl rollout restart deployment/frontend -n $NAMESPACE
        '''
      }
    }

    stage('Verify Deployment') {
      steps {
        sh '''
          kubectl get pods -n $NAMESPACE
          kubectl get svc -n $NAMESPACE
        '''
      }
    }
  }

  post {
    success {
      echo '✅ CI/CD pipeline completed successfully'
    }
    failure {
      echo '❌ CI/CD pipeline failed'
    }
  }
}
