FROM rust

# Install docker cli
RUN apt-get update
RUN apt-get install ca-certificates curl gnupg lsb-release -y
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install docker-ce-cli docker-compose-plugin -y

# Install protoc
RUN apt install protobuf-compiler -y

CMD cargo build --package dozer-tests --bin dozer-tests --bin dozer-test-client && ./target/debug/dozer-tests -r buildkite
