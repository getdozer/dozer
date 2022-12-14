## Troubleshooting

#### M1/M2 Apple Silicon Related
Our binary has dependency on `unixodbc`, `openssl@1.1`.

Following error might be encountered while installing `unixodbc`, `openssl@1.1` using `Homebrew`.
```
Error: Cannot install under Rosetta 2 in ARM default prefix (/opt/homebrew)!
To rerun under ARM use:
arch -arm64 brew install ...
To install under x86_64, install Homebrew into /usr/local.
```
To solve this,

First, install `/usr/local/` based x86_64 `Homebrew` (you can skip if you already have `Homebrew` in `/usr/local/` directory).
```bash
arch -x86_64 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```
Second, install `unixodbc`, `openssl@1.1` using following command
```bash
arch -x86_64 /usr/local/Homebrew/bin/brew install unixodbc
```
```bash
arch -x86_64 /usr/local/Homebrew/bin/brew install openssl@1.1
```
