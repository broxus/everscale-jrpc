<p align="center">
  <a href="https://github.com/venom-blockchain/developer-program">
    <img src="https://raw.githubusercontent.com/venom-blockchain/developer-program/main/vf-dev-program.png" alt="Logo" width="366.8" height="146.4">
  </a>
</p>


## Everscale JRPC module

This module implements a JSON-RPC (JRPC) light server protocol
for the Everscale/Venom blockchains.

### Contents

- [`everscale-rpc-models`](./models) - models used on servers and clients.
- [`everscale-rpc-proto`](./proto) - Protobuf models used for binary transport.
- [`everscale-rpc-server`](./server) - module implementation.
- [`everscale-rpc-client`](./client/README.md) - JRPC client implementation.
- [`everscale-rpc-node`](./node/README.md) - a simple light node with the JRPC module.

## Methods

- [`getCapabilities`](#getcapabilities)
- [`getLatestKeyBlock`](#getlatestkeyblock)
- [`getBlockchainConfig`](#getblockchainconfig)
- [`getStatus`](#getstatus)
- [`getTimings`](#gettimings)
- [`getContractState`](#getcontractstate)
- [`sendMessage`](#sendmessage)
- [`getTransactionsList`](#gettransactionslist)
- [`getTransaction`](#gettransaction)
- [`getDstTransaction`](#getdsttransaction)
- [`getAccountsByCodeHash`](#getaccountsbycodehash)

### `getCapabilities`

Returns a list of supported methods of the node

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getCapabilities",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of supported methods
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getCapabilities",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
      "getCapabilities",
      "getLatestKeyBlock",
      "getBlockchainConfig",
      "getStatus",
      "getTimings",
      "getContractState",
      "sendMessage",
      "getTransactionsList",
      "getTransaction",
      "getDstTransaction",
      "getAccountsByCodeHash"
    ]
  }
  ```
</details>

---

### `getLatestKeyBlock`

Retrieves the latest key block from the blockchain.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getLatestKeyBlock",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "block": string // Base64 encoded key block
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getLatestKeyBlock",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc":"2.0",
    "id": 1,
    "result":{
      "block":"te6ccgICBN4AAQAAs...V5kdk/9OjYskr9go="
    }
  }
  ```
</details>

---

### `getBlockchainConfig`

Retrieves the latest blockchain config from the blockchain.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getBlockchainConfig",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "globalId": number, // Network id
    "config": string // Base64 encoded config params
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getBlockchainConfig",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc":"2.0",
    "id":1,
    "result":{
      "globalId": 42,
      "config": "te6ccgICAQQAAQAAH6w...VVVVVVVVVVVV"
    }
  }
  ```
</details>

---

### `getStatus`

Returns a node JRPC module status.

> Not applicable to public endpoint

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getStatus",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "ready": boolean, // Whether the JRPC module is ready
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getStatus",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "ready": true
    }
  }
  ```
</details>

---

### `getTimings`

Returns node state timings.

> Not applicable to public endpoint

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTimings",
  "params": {}
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "last_mc_block_seqno": number, // Number of the latest known masterchain block
    "last_shard_client_mc_block_seqno": number, // Number of the latest fully processed masterchain block
    "last_mc_utime": number, // Unix timestamp of the latest known masterchain block
    "mc_time_diff": number, // Lag of the latest known masterchain block
    "shard_client_time_diff": number, // Lag of the latest fully processed masterchain block
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTimings",
    "params": {}
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
      "last_mc_block_seqno": 13501208,
      "last_shard_client_mc_block_seqno": 13501208,
      "last_mc_utime": 1687966105,
      "mc_time_diff": 3,
      "shard_client_time_diff": 5
    }
  }
  ```
</details>

---

### `getContractState`

Retrieves the state of an account at the specific address.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getContractState",
  "params": {
    "address": string // Contract address
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": {
    "type": "notExists"
  } | {
    "type": "exists",
    "account": string, // Base64 encoded account state (AccountStuff model)
    "lastTransactionId": {
      "isExact": boolean,
      "hash": string, // Hex encoded transaction hash
      "lt": string, // Transaction logical time (u64)
    },
    "timings": {
      "genLt": string, // Logical time of the shard state for the requested account
      "genUtime": number, // Unix timestamp of the shard state for the requested account
    }
  }
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getContractState",
    "params": {
      "address": "-1:3333333333333333333333333333333333333333333333333333333333333333"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id":1,
    "result": {
      "type": "exists",
      "account": "te6ccgICA94AAQAA...9AT6ANMf0//R",
      "lastTransactionId": {
        "isExact":true,
        "lt":"39526989000002",
        "hash":"55d7913633ba7d6c9d10228a0ce1586415c781ae855cbd8df4a272683cd86030"
      },
      "timings": {
        "genLt": "39526989000002",
        "genUtime": 1687964202
      }
    },
  }
  ```
</details>

---

### `getAccountsByCodeHash`

Retrieves a sorted list of addresses for contracts with the specified code hash.

Use the last address from the list for the `continuation`.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getAccountsByCodeHash",
  "params": {
    "codeHash": string, // Hex encoded code hash
    "limit": number, // Max number of items in response (at most 100)
    "continuation": string | undefined, // Optional address as a continuation (>)
  },
  "id": 1
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of addresses
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountsByCodeHash",
    "params": {
      "codeHash": "9c1018fd3cca838d90d60f1ff5ac1c7648f3a854c70b769b2eebae8d3854722b",
      "limit": 100
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
      "0:02ccc13022392b30ee4ffd3cdb8910ce7698a9e98deb1f4c47519a69079780f2",
      "0:09f9d103e4db8c0c4d950480ac4ce508a09ce463ec1ab0a5beec63f235b75039",
      "0:10ab10aec7fe23cad95e369e581a87d6d04cfbc27014e6ed051028dfadc8b447",
      "0:3b9788bd584863221e665e237614d7668ca85c3fef001697df75bc4487dd60b3",
      "0:6743e9f3c9bb3370da013a5c4e85fdd8f92c1d99223d54d6948dc3278b1bd280",
      "0:7dba04bc2c1e8bec852595db8208c089063829bb76867f33770c1a86959de396",
      "0:a7b82ea9b68bb07730f09c580fa1a8a973e9aa6ae55fd5af2cbde3d8a5c6bf0e",
      "0:e6b79f7783d7c02bddbb858670e6dff58c997b646f33f97eabbd38d558424ed6",
      "0:f525f80bff5b6eaae047fd679403ca0dac3e5a4ce221f9b89beb06f5fbb9c899",
      "0:ff22424953f5b51d0029a930b09cb949a639fdeaefda48ec990f51c46a743141"
    ]
  }
  ```
</details>

---

### `getTransactionsList`

Retrieves a list of raw transactions in the descending order.

Use the `prevTransactionLt` the last transaction in the list for the `continuation`.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTransactionsList",
  "params": {
    "account": string, // Account address
    "limit": number, // Max number of items in response (at most 100)
    "lastTransactionLt": string | undefined, // Optional logical time to start from (>=)
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string[], // List of base64 encoded transactions
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransactionsList",
    "params": {
      "account": "-1:3333333333333333333333333333333333333333333333333333333333333333",
      "limit": 10
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
      "te6ccgECBwEAAYk...AMk4nHRA",
      "te6ccgECBgEAASw...ybpEAAEg",
      "te6ccgECBwEAAYk...gMk4nG5A",
      "te6ccgECBgEAASw...omdqAAEg",
      "te6ccgECBwEAAYk...AMk4nGpA",
      "te6ccgECBgEAASw...+qa1AAEg",
      "te6ccgECBwEAAYk...gMk4nGRA",
      "te6ccgECBgEAASw...BbJmAAEg",
      "te6ccgECBwEAAYk...fMk4nFxA",
      "te6ccgECBgEAASw...SZ2RAAEg"
    ],
    "id": 1
  }
  ```
</details>

---

### `getTransaction`

Searches for a transaction by the id.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getTransaction",
  "params": {
    "id": string, // Hex encoded transaction hash
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": string | undefined, // Optional base64 encoded transaction
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": {
      "id": "26380563b9df3833d73574a93e9c5f833118561d821d08da44fa2d8549c90cbc"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": "te6ccgECBw...t1AMk4nk5A"
  }
  ```
</details>

---

### `getDstTransaction`

Searches for a transaction by the id of an incoming message.

> Requres the full API mode

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "getDstTransaction",
  "params": {
    "messageHash": string, // Hex encoded message hash
  }
}
```

**Response:**
```typescript
{
  "jsonrpc":"2.0",
  "id": number, // Request id
  "result": string | undefined, // Optional base64 encoded transaction
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getDstTransaction",
    "params": {
      "messageHash": "c56e30355295c9de31bb82d893c06008aaf63194fb4b4ab7c7dd9f9f20666f60"
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": "te6ccgECBwEAAY...AMk4nk5A"
  }
  ```
</details>

---

### `sendMessage`

Broadcasts an external message.

**Request:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "method": "sendMessage",
  "params": {
    "message": string, // Base64 encoded message
  }
}
```

**Response:**
```typescript
{
  "jsonrpc": "2.0",
  "id": number, // Request id
  "result": null,
}
```

<details>
  <summary>Example</summary>

  **Request:**
  ```bash
  curl -X POST -H "Content-Type: application/json" https://jrpc.everwallet.net -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "sendMessage",
    "params": {
      "message": "te6ccgEBAQEAdQAA5Yg...+nNzEuRY="
    }
  }'
  ```

  **Response:**
  ```json
  {
    "jsonrpc": "2.0",
    "id": 1,
    "result": null
  }
  ```
</details>

---
