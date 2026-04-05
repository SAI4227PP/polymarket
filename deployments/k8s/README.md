# Kubernetes Deploy

## Build and load images (local cluster)
- `docker build -f deployments/docker/Dockerfile.api -t polymarket-bot-api:latest .`
- `docker build -f deployments/docker/Dockerfile.supervisor -t polymarket-bot-supervisor:latest .`
- `docker build -f deployments/docker/Dockerfile.trader -t polymarket-bot-trader:latest .`

## Apply manifests
- `kubectl apply -f deployments/k8s/namespace.yaml`
- `kubectl apply -f deployments/k8s/redis.yaml`
- `kubectl apply -f deployments/k8s/configmap.yaml`
- `kubectl apply -f deployments/k8s/api-deployment.yaml`
- `kubectl apply -f deployments/k8s/supervisor-deployment.yaml`
- `kubectl apply -f deployments/k8s/trader-deployment.yaml`

## Verify
- `kubectl get pods -n polymarket-bot`
- `kubectl logs -n polymarket-bot deploy/trader`
- `kubectl logs -n polymarket-bot deploy/api`
