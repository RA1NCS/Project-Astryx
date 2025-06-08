# Default target - builds and deploys
make: build deploy

# Build Docker image
build:
	docker build --platform linux/amd64 -t astryxcontainers.azurecr.io/worker:latest .

# Deploy to Azure Container Registry
deploy:
	az acr login --name astryxcontainers && docker push astryxcontainers.azurecr.io/worker:latest && az containerapp update --name astryx-ingestion --resource-group learning --image astryxcontainers.azurecr.io/worker:latest

.PHONY: make build deploy
