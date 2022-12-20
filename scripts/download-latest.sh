#!/bin/sh

# GLOBALS

# Colors
RED='\033[31m'
GREEN='\033[32m'
DEFAULT='\033[0m'

# Project name
PNAME='dozer'

# ARGUMENTS

while [[ $# -gt 0 ]]; do
  case $1 in
    -d|--docker)
      use_docker=true
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# FUNCTIONS

# Gets the OS by setting the $os variable
# Returns 0 in case of success, 1 otherwise
get_os() {
    os_name=$(uname -s)
    case "$os_name" in
    'Darwin')
        os='macos'
        ;;
    'Linux')
        os='linux'
        ;;
    *)
        return 1
    esac
    return 0
}

success_download() {
    printf "$GREEN%s\n$DEFAULT" "Dozer $latest binary successfully downloaded as '$binary_name' file."
}

success_unzip_remove() {
    printf "$GREEN%s\n$DEFAULT" "Dozer $latest binary successfully extracted as 'dozer' folder."
    echo ''
    echo 'Run it:'
    echo "    $ .dozer/$PNAME"
    echo 'Usage:'
    echo "    $ .dozer/$PNAME --help"
}

not_available_failure_usage() {
    printf "$RED%s\n$DEFAULT" 'ERROR: Dozer binary is not available for your OS distribution or your architecture yet.'
    echo ''
    echo 'However, you can easily compile the binary from the source files.'
    echo 'Follow the steps at the page ("Source" tab): https://docs.dozer.com/learn/getting_started/installation.html'
}

fetch_release_failure_usage() {
    echo ''
    printf "$RED%s\n$DEFAULT" 'ERROR: Impossible to get the latest stable version of Dozer.'
    echo 'Please let us know about this issue: https://github.com/getdozer/dozer/issues/new/choose'
    echo ''
    echo 'In the meantime, you can manually download the appropriate binary from the GitHub release assets here: https://github.com/getdozer/dozer/releases/latest'
}

fill_release_variables() {
    # Fill $latest variable
    latest='latest'

     # Fill $os variable
     if ! get_os; then
        not_available_failure_usage
        exit 1
     fi

     # Fill $archi variable
     archi='amd64'
}

download_binary() {
  fill_release_variables
    if [ "$use_docker" = true ] ; then
        echo "Downloading Dozer $latest source code..."

        base_url="https://drive.google.com/uc?export=download&id=1uVmZsZ7ex6jMf2LM-QzJj0S43nqUQq2e"
        binary_name="$PNAME.tar.gz"

        curl -L "$base_url" -o $binary_name
        chmod 744 "$binary_name"
        success_download

        tar -xvf $binary_name
        rm -f $binary_name
        mv $PNAME-$latest/ $PNAME
        cp ./$PNAME/tests/simple_e2e_example/docker-compose.yml ./$PNAME/docker-compose.yml
        success_unzip_remove
    else
        echo "Downloading Dozer binary $latest for $os, architecture $archi..."
        case "$os" in
            'macos')
                base_url="https://drive.google.com/uc?export=download&id=1SWlt8tpXAtF5O0ZL5qnAnLfp-boja7IE"
                release_file="$PNAME-$os-$archi-$latest.tar.gz"
                binary_name="$PNAME.tar.gz"
                ;;
            'linux')
                base_url="https://drive.google.com/uc?export=download&id=1vESgAKwZ4yDlGj-aOkqI6uH7_nh6Ua45"
                release_file="$PNAME-$os-$archi-$latest.tar.gz"
                binary_name="$PNAME.tar.gz"
                ;;
            *)
                return 1
        esac

        # Fetch the Dozer binary
        curl --fail -L "$base_url" -o $binary_name
        if [ $? -ne 0 ]; then
            fetch_release_failure_usage
            exit 1
        fi
        mv "$release_file" "$binary_name"
        chmod 744 "$binary_name"
        success_download

        tar -xvf $binary_name
        rm -f $binary_name
        mv $PNAME-$os-$archi-$latest/ $PNAME
        success_unzip_remove
    fi
}

# MAIN

main() {
    download_binary
}
main