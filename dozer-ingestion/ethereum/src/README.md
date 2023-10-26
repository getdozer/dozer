```
docker run -d --name ethereum-node -v "$PWD":/root \
           -p 8545:8545 -p 30303:30303 \
           ethereum/client-go --http.addr 0.0.0.0 --gcmode archive --syncmode full --txlookuplimit 0 


docker run -v "$PWD":/root \
           -p 8545:8545 -p 30303:30303 -p 3334:3334 -p 8551:8551 \
           ethereum/client-go --http.addr 0.0.0.0 --gcmode archive --syncmode full --txlookuplimit 0 --ws --ws.port 3334 --ws.api eth,net,web3 --ws.origins '*'

 
```