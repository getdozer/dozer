set -e

apt install -y build-essential
sh .github/workflows/integration/test-dozer.sh
