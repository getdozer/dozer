# Real-time Ethereum Sample

## Initialize List of Ethereum Addresses

```curl
# pwd -> dozer-samples/ethereum-sample

./scripts/download_eth_addresses.sh
```

```bash
docker-compose -f docker-compose.yml up
```

## Run Dozer

```bash
docker run -it \
  -v "$PWD":/usr/dozer \
  -p 8080:8080 \
  -p 50051:50051 \
  --platform linux/amd64 \
  --env ETH_HTTPS_URL=<HTTPS_URL> \
  --env ETH_WSS_URL=<WSS_URL> \
  public.ecr.aws/getdozer/dozer:dev \
  dozer
```

docker run -it \
-v "$PWD":/usr/dozer \
-p 8080:8080 \
-p 50051:50051 \
--platform linux/amd64 \
--env ETH_HTTPS_URL="https://patient-blissful-lambo.quiknode.pro/91542c54a5c4926208e48b950c03147449e86c0c" \
--env ETH_WSS_URL="wss://patient-blissful-lambo.quiknode.pro/91542c54a5c4926208e48b950c03147449e86c0c" \
public.ecr.aws/getdozer/dozer:dev \
dozer

