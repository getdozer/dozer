## Publish ethereum smart contract data as APIs

Features used:
- Initialize project from [yaml](./docker-compose.yml) with docker
- Setup source with real time eth stats
- SQL execution and gRPC & REST API endpoints creation from [yaml](./dozer-config.yaml)

[//]: # (- Creation of embeddable React widget)

### Running

Register with a websocket provider such as Infura and initialize the env varialbe `ETH_WSS_URL`. 
```
export ETH_WSS_URL=<WSS_URL>

# or using ganache

ganache fork
export ETH_WSS_URL="ws://localhost:8545"
```

Run the sample
```
docker-compose up
```

### References
- [Fork Ethereum with Ganache]((https://docs.infura.io/infura/tutorials/ethereum/fork-ethereum-with-ganache) )
