# node:alpine will be our base image to create this image
FROM node:alpine
# Set the /app directory as working directory
WORKDIR /app

RUN apk --no-cache add curl

# Install ganache-cli globally
RUN npm install -g ganache
# Running as an archive node from the block specifiec in the dozer-config.yaml 
# https://docs.infura.io/infura/tutorials/ethereum/fork-ethereum-with-ganache

CMD ["ganache", "-h", "0.0.0.0", "-m", "marriage tiny prepare canyon grape half kingdom guide desert surge three nurse"]
