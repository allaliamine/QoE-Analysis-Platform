pipeline {
  agent any

  environment {
    CLUSTER_NAME = "qoe"
    NAMESPACE    = "qoe"

    VENV_DIR = ".venv"
    PATH = "/opt/homebrew/bin:/usr/local/bin:/bin:/usr/bin:/sbin:/usr/sbin"
    PYTHON_PATH  = "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3.13"

    BACKEND_IMAGE  = "qoe/dashboard-backend:dev"
    FRONTEND_IMAGE = "qoe/dashboard-frontend:dev"
  }

  options {
    timestamps()
  }

  stages {

    stage('Tool Check') {
        steps {
            sh '''
            echo "PATH=$PATH"
            which docker
            which kind
            which kubectl
            docker version
            kind version
            kubectl version --client
            '''
        }
    }

    stage('Checkout') {
        steps {
            git branch: 'main',
            url: 'https://github.com/allaliamine/QoE-Analysis-Platform.git'
        }
    }


    stage('Setup Virtual Environment') {
        steps {
            sh """
            ${PYTHON_PATH} -m venv ${VENV_DIR}
            . ${VENV_DIR}/bin/activate
            pip install --upgrade pip
            pip install -r requirements.txt
            """
        }
    }

    stage('Run Tests') {
      steps {
            sh """
            . ${VENV_DIR}/bin/activate
            pytest tests/
            """
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

          kubectl apply -f ./infrastructure/kind/app/backend.yaml
          kubectl apply -f ./infrastructure/kind/app/frontend.yaml

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
