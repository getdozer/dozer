## End-to-end Ethereum Stats Example

Features used:
- Initialize project from [yaml](./docker-compose.yml) with docker
- Setup source with real time eth stats
- SQL execution and gRPC & REST API endpoints creation from [yaml](./dozer-config.yaml)

[//]: # (- Creation of embeddable React widget)

### Run

```bash
docker-compose up --build

# Note: For Apple silicons pre-setting
export DOCKER_DEFAULT_PLATFORM=linux/amd64
```
