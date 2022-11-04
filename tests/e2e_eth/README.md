### Providing APIs on top of Ethereum Data


### Build Dependencies
- [`Ganache`](https://github.com/trufflesuite/ganache)

### Install & Run Ganache
```
npm install ganache --global

ganache -m "quote mandate cliff boil scheme abstract monitor bike other destroy panic abandon"
```

```
ganache --fork https://wiser-smart-sound.ethereum-goerli.discover.quiknode.pro/c94c6154019a91660db7f9d2b718622d3355e471/
```
https://goerli.etherscan.io/tx/0x2578c4ff3ee0ea6907cae77b4d6c89d2a56e3e48bd17b408438bfb93ba189b0e

```
 curl -H 'Content-Type: application/json'   --data '{"jsonrpc":"2.0", "id": 1, "method": "eth_getBalance", "params": ["0x8dc847af872947ac18d5d63fa646eb65d4d99560"] }' http://localhost:8545

````

https://medium.com/mycrypto/understanding-event-logs-on-the-ethereum-blockchain-f4ae7ba50378