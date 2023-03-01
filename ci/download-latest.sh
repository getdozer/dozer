#!/bin/sh

# GLOBALS

# Colors
RED='\033[31m'
GREEN='\033[32m'
DEFAULT='\033[0m'

# Project name
PNAME='dozer'

# GitHub API address
GITHUB_API='https://api.github.com/repos/getdozer/dozer/releases'
# GitHub Release address
GITHUB_REL='https://github.com/getdozer/dozer/releases/latest/download/'

# FUNCTIONS

# Gets the version of the latest stable version of Dozer by setting the $latest variable.
# Returns 0 in case of success, 1 otherwise.
get_latest() {
    # temp_file is needed because the grep would start before the download is over
    temp_file=$(mktemp -q /tmp/$PNAME.XXXXXXXXX)
    latest_release="$GITHUB_API/latest"

    if [ $? -ne 0 ]; then
        echo "$0: Can't create temp file."
        fetch_release_failure_usage
        exit 1
    fi

    if [ -z "$GITHUB_PAT" ]; then
        curl -s "$latest_release" > "$temp_file" || return 1
    else
        curl -H "Authorization: token $GITHUB_PAT" -s "$latest_release" > "$temp_file" || return 1
    fi

    latest="$(cat "$temp_file" | grep '"tag_name":' | cut -d ':' -f2 | tr -d '"' | tr -d ',' | tr -d ' ')"

    rm -f "$temp_file"
    return 0
}

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

# Gets the architecture by setting the $archi variable.
# Returns 0 in case of success, 1 otherwise.
get_archi() {
    architecture=$(uname -m)
    case "$architecture" in
    'x86_64' | 'amd64' )
        archi='amd64'
        ;;
    'arm64')
        # macOS M1/M2
        if [ $os = 'macos' ]; then
            archi='mac'
        else
            archi='mac-intel'
        fi
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
    # Fill $latest variable.
    if ! get_latest; then
        fetch_release_failure_usage
        exit 1
    fi
    if [ "$latest" = '' ]; then
        fetch_release_failure_usage
        exit 1
     fi
     # Fill $os variable.
     if ! get_os; then
        not_available_failure_usage
        exit 1
     fi
     # Fill $archi variable.
     if ! get_archi; then
        not_available_failure_usage
        exit 1
     fi
}

download_binary() {
    fill_release_variables
    binary_name="$PNAME"
    case "$os" in
    'macos')
        release_file="$PNAME-$archi-$latest.tar.gz"
        ;;
    'linux')
        release_file="$PNAME-$os-$archi-$latest.tar.gz"
        ;;
    *)
        return 1
    esac

    echo "Downloading Dozer binary $latest for $os, architecture $archi -- $release_file..."

    # Fetch the Dozer binary.
    curl --fail -OL "$GITHUB_REL/$release_file"
    chmod 744 "$binary_name"
}

# MAIN
main() {
    download_binary
}
main
