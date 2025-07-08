# Default target - builds and deploys
make: build deploy

# Build Docker image
build:
	docker build --platform linux/amd64 -t astryxcontainers.azurecr.io/worker:latest .

# Deploy to Azure Container Registry
deploy:
	az acr login --name astryxcontainers && docker push astryxcontainers.azurecr.io/worker:latest && az containerapp update --name astryx-ingestion --resource-group learning --image astryxcontainers.azurecr.io/worker:latest

empty:
	az storage blob delete-batch --account-name astryxstoringest --source raw
	az storage blob delete-batch --account-name astryxstoringest --source processed
	weaviate-cli delete collection --collection TextChunk
	weaviate-cli delete collection --collection ImageChunk

.PHONY: make build deploy
