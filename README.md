<p align="center">
  <a href="https://github.com/venom-blockchain/developer-program">
    <img src="https://raw.githubusercontent.com/venom-blockchain/developer-program/main/vf-dev-program.png" alt="Logo" width="366.8" height="146.4">
  </a>
</p>


## Everscale JRPC module

Everscale-jrpc is a module that implements a subset of the JSON-RPC (JRPC)
protocol for the Everscale blockchain. It allows nodes in the Everscale network
to communicate with each other and with off-chain applications using JRPC. The
module includes a set of pre-defined methods that can be called using JRPC, such
as sendMessage, which allows a node to send a message within the Everscale
blockchain. The module is built into nodes and can be accessed via the node's
API.

### Contents

- [`everscale-jrpc-models`](./models) - models used on server and client.
- [`everscale-jrpc-sever`](./server) - base implementation integrated into light
  nodes.
- [`everscale-jrpc-client`](./client/README.md) - jrpc client implementation.

## Methods

### `getLatestKeyBlock`

This methos is used to retrieve the latest key block from the Everscale. Key
blocks are a type of block in a blockchain that contain important information,
such as the state of the network and the current validator set.

To call the getLatestKeyBlock method using JSON-RPC, the client must send a
request to the server in the following format:

```
{
  "jsonrpc": "2.0",
  "method": "getLatestKeyBlock",
  "params": {},
  "id": 1
}
```

The server will return a JSON-RPC response with the following format:

```

{ "jsonrpc": "2.0", "result": "<key_block>", "id": 1 }

```

`jsonrpc` (string) - specifies the version of the JSON-RPC protocol being used.

`method`(string) - specifies the name of the method being called, in this case
`getLatestKeyBlock`.

`params` (dict) - contains an object with any required parameters for the
method. In this case, no parameters are required, so the object is empty.

`id` (number) - a unique identifier for the request that is used to match the
request with the corresponding response from the server.

<details>
  <summary>  Here is an example of how to call the getLatestKeyBlock method from a shell,
you can use a tool like curl to send a POST request to the server with the
JSON-RPC specification as the body of the request: </summary>

```
curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc": "2.0", "method": "getLatestKeyBlock", "params": {}, "id": 1}' https://jrpc.everwallet.net/rpc

```

  </details>

  <details>
    <summary>Here is an example of how you might call the getLatestKeyBlock method using
  the `fetch` or `node-fetch` library in JavaScript:</summary>

```

const url = "https://jrpc.everwallet.net/rpc";

const request = {
  jsonrpc: "2.0",
  method: "getLatestKeyBlock",
  params: {},
  id: 1,
};

fetch(url, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
  },
  body: JSON.stringify(request),
})
  .then(response => (response.ok ? response.json() : Promise.reject(response)))
  .then(response => console.log(response.result))
  .catch(error => console.error(error));

```

  </details>

   <details>
    <summary>And here is an example of how to call the getLatestKeyBlock method using Python:</summary>

```
import requests

response = requests.post(
  "https://jrpc.everwallet.net/rpc",
  json={
    'jsonrpc': '2.0',
    'method': 'getLatestKeyBlock',
    'params': {},
    'id': 1,
  },
)
print(response.json()["result"])

```

  </details>

   <details>
    <summary>Result:</summary>

```
  {block: 'te6ccgICBRYAAQAAu9IAAAQQEe9VqgAAACoFFAUOA8kAA…01fla/5WN/+81jQ//FTeVjDAcBAhv2TZFLEs3BFDF7wo='}
```

  </details>

### `getContractState`

This method is used to retrieve the state of a contract at a specific address in
the Everscale blockchain. The method takes a single parameter, the contract
address, and returns information about the contract's account, timings, and last
transaction ID.

To call the `getContractState` method using JSON-RPC, you will need to send a
request to the server in the following format:

```
{
  "jsonrpc": "2.0",
  "method": "getContractState",
  "params": {
    "address": <address>
  },
  "id": 1
}

```

`address` (string) - The parameter is required, and it should be a string
representing the address of the contract for which the state is being retrieved.

The server will return a JSON-RPC response with the following format:

```

{
  "jsonrpc": "2.0",
  "result": "
    {
      "account": <encoded_base64>,
      "lastTransactionId": {
        "hash": <hex_encoded>,
        "isExact": <bool>,
        "lt": <timestamp>
      },
      timings: {
        "genLt": <logical_time>,
        "genUtime": <timestamp>
      },
      type: <exists | notExists>
    }",
  "id": <number>
}

```

  <details>
    <summary>Example request (JavaScript):</summary>

```
const url = "https://jrpc.everwallet.net/rpc";

const request = {
  jsonrpc: "2.0",
  method: "getContractState",
  params: {
    "address": "-1:3333333333333333333333333333333333333333333333333333333333333333"
  },
  id: 1,
};

fetch(url, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
  },
  body: JSON.stringify(request),
})
  .then(response => (response.ok ? response.json() : Promise.reject(response)))
  .then(response => console.log(response))
  .catch(error => console.error(error));

```

  </details>

  <details>
    <summary>Example request (Python):</summary>

```
import requests

response = requests.post(
  "https://jrpc.everwallet.net/rpc",
  json={
    'jsonrpc': '2.0',
    'method': 'getContractState',
    'params': {
      "address": "-1:3333333333333333333333333333333333333333333333333333333333333333"
    },
    'id': 1,
  },
)
print(response.json())
```

  </details>

  <details>
    <summary>Result:</summary>

```
{account: 'te6ccgICA9kAAQAAvacAAAJ7n+ZmZmZmZmZmZmZmZmZm…9AAS9AAB+gLLH8v/ye1UACDtRND0BPQE9AT6ANMf0//R', lastTransactionId: {…}, timings: {…}, type: 'exists'}
```

  </details>

### `getAccountsByCodeHash`

This method is used to retrieve a list of accounts that have a specific code
hash. The method takes two parameters: a code hash and a limit on the number of
accounts to retrieve. The method also takes an optional third parameter, a
continuation token, which can be used to retrieve additional accounts beyond the
specified limit. The method returns a list of addresses for the accounts that
match the code hash.

To call the `getAccountsByCodeHash` method using JSON-RPC, you will need to send
a request to the server in the following format:

```
{
  "jsonrpc": "2.0",
  "method": "getAccountsByCodeHash",
  "params": {
    "codeHash": <hex_encoded>
  },
  "id": 1
}
```

`code_hash` (string) - the code hash of the contracts you want to find. This
should be a hex-encoded byte array.

`limit` (number) - the maximum number of accounts to return in a single batch.

`continuation` (string, optional) - the last address from the previous batch, if
you are requesting more than one batch of results. This is optional and should
be set to null if you are requesting the first batch of results.

The server will return a JSON-RPC response with the following format:

```
{
  "jsonrpc": "2.0",
  "result": "<address []>",
  "id": 1
}

```

  <details>
    <summary>Example request (shell):</summary>

```
curl -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","method":"getAccountsByCodeHash","params":{"codeHash":"e8801282233f38ad17fead83aeac1820e40598de98675106b0fd91d9524e9141","limit":10},"id":1}' https://jrpc.everwallet.net/rpc

```

  </details>

  <details>
    <summary>Result</summary>

```
{"jsonrpc":"2.0","result":
["0:005bb79c292a968bccedae3ded1a24cc1a483c0b2cd98a80c199ba1c0a22b472","0:007433bd4e4f8e223890cf757fe047a68f0aee2fdef4900063c6c1a5493ad9d1","0:00b8698b42b8b6b00815ddf52d2b1893b1819bee17045ae5ab86bfb4546f236e","0:00ca16398f314a9b3bed582dc69582515d866ededb6c4e18190f63b305cedf91","0:010f7a877ad90df1f7111e9431c672fc0d68714fcded79c61369765396adc802","0:0126b34560539a1fa6b3f5954230b302185e9b9be73e870c3a021e04165ef777","0:01c8c8c678456e3bfbda8c8f655276b39af278a6527f3c42d8d1076ee6a68f5c","0:01d0e9a908dbd7130350c8a1eb1abd77b3efe15c43f868fdd3cf6523cfcf0dff","0:0231065123ddbe5292302ca527588984d9a2bcfa3c91da6657666dc33cf0e560","0:024aa7a03dcf20bba5d486af1151f4a33fc882b5300f185bf4a896bc3e20dd19"],"id":1
}
```

  </details>

  <details>
    <summary>Example request (JavaScript):</summary>

```
const request = {
  jsonrpc: "2.0",
  method: "getAccountsByCodeHash",
  params: {
    "codeHash": "e8801282233f38ad17fead83aeac1820e40598de98675106b0fd91d9524e9141",
    "limit": 10,
    "continuation": "010f7a877ad90df1f7111e9431c672fc0d68714fcded79c61369765396adc802",
  },
  id: 1
};

fetch("https://jrpc.everwallet.net/rpc", {
  method: "POST",
  body: JSON.stringify(request)
})
  .then(response => response.json())
  .then(result => console.log(result.result))
  .catch(error => console.error(error));

```

Example request (Python):

```
import json
import requests

request = {
  "jsonrpc": "2.0",
  "method": "getAccountsByCodeHash",
  "params":  {
    "codeHash": "e8801282233f38ad17fead83aeac1820e40598de98675106b0fd91d9524e9141",
    "limit": 10,
    "continuation": "010f7a877ad90df1f7111e9431c672fc0d68714fcded79c61369765396adc802",
  },
  "id": 1
}

response = requests.post("https://jrpc.everwallet.net/rpc", json=request)
result = response.json()
print(result["result"])

```

  </details>

  <details>
    <summary>Result</summary>

```
['0:0126b34560539a1fa6b3f5954230b302185e9b9be73e870c3a021e04165ef777', '0:01c8c8c678456e3bfbda8c8f655276b39af278a6527f3c42d8d1076ee6a68f5c', '0:01d0e9a908dbd7130350c8a1eb1abd77b3efe15c43f868fdd3cf6523cfcf0dff', '0:0231065123ddbe5292302ca527588984d9a2bcfa3c91da6657666dc33cf0e560', '0:024aa7a03dcf20bba5d486af1151f4a33fc882b5300f185bf4a896bc3e20dd19', '0:0263b6ced80026bb59925bc94babb9efebf33660ad3c2c3159b1b6fe3f4dc8f9', '0:026c76fe98daf133853d36327fe1d526825af387c2a5d67895fb8baaf657d26f', '0:02a08a2db83c7975647711acb4947017f5c7f38c75c755f3e1d435c2e37d5dc8', '0:02a5a10df13f0d6920753a3c234f6a6f99d427c0c10369a4f85c5f9867562be7', '0:02cc62df673846d60e5a56eeda3225486bb7fb13d168e70416ea65a66e67a708']
```

  </details>

### `getTransactionsList`

This method is used to retrieve a list of transactions for a specific account.
To call this method, you will need to send a JSON-RPC request to the server with
the following structure:

```
{
  "jsonrpc": "2.0",
  "method": "getTransactionsList",
  "params": {
    "account": <address>,
    "limit": <number>,
    "last_transaction_lt": Option<logical_time>
  },
  "id": <number>
}

```

The request parameters include:

`account` (string) - the address of the account for which to retrieve the
transactions

`limit` (number) - the maximum number of transactions to return

`last_transaction_lt` (optional) - the logic time of the last transaction to
return. If not provided, all transactions will be returned.

The server will return a JSON-RPC response with the following format:

```

{
  "jsonrpc": "2.0",
  "result": <base64 []>,
  "id": <number>
}

```

  <details>
    <summary>Example (Shell):</summary>

```
curl -X POST -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0",
  "method": "getTransactionsList",
  "params": {
    "account": "-1:3333333333333333333333333333333333333333333333333333333333333333",
    "limit": 10
  },
  "id": 1
}' https://jrpc.everwallet.net/rpc

```

  </details>

  <details>
    <summary> Example (JavaScript):</summary>

```
const url = "https://jrpc.everwallet.net/rpc";

const request = {
  jsonrpc: "2.0",
  method: "getTransactionsList",
  params: {
    account:
      "0:a519f99bb5d6d51ef958ed24d337ad75a1c770885dcd42d51d6663f9fcdacfb2",
    limit: 10,
  },
  id: 1,
};

fetch(url, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(request),
})
  .then(response => response.json())
  .then(response => console.log(response.result));

```

  </details>

  <details>
    <summary>Example (Python):</summary>

```
import json
import requests

request = {
  "jsonrpc": "2.0",
  "method": "getTransactionsList",
  "params":  {
    account:
      "0:a519f99bb5d6d51ef958ed24d337ad75a1c770885dcd42d51d6663f9fcdacfb2",
    limit: 10,
  },
  "id": 1
}

response = requests.post("https://jrpc.everwallet.net/rpc", json=request)
result = response.json()
print(result["result"])
```

  </details>

  <details>
    <summary>Result:</summary>

```
(10)[
  'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3CIXc1…Vb4tn5pQUA8AMKVQ6UR93A0SYUx7BuorhquQAOkIAAAAOA==',
  'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3CIXc1…Vb4tn5pQUA8AMDJ13QZxSM8XbmNrzWZQaQKL2o7pAAAAOA==',
  ...
  'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3CIXc1…Vb4tn5pQUA8AME+ZV+jcWMvEGSnEzguWJqZbejakAAAAOA=='
]
```

  </details>

  <details>
    <summary> How extract all transactions with ever-sdk-js:</summary>

```
import fetch from "node-fetch";
import { TonClient } from "@eversdk/core";
import { libNode } from "@eversdk/lib-node";
TonClient.useBinaryLibrary(libNode);

const client = new TonClient({
  network: {
    endpoints: ["net.ton.dev"],
  },
});

const url = "https://jrpc.everwallet.net/rpc";

async function getAllTransactions(account) {
  let transactions = await getTransactionsList(account);

  while (true) {
    const oldestTx = transactions[transactions.length - 1];
    const parsedOldestTransaction = await client.boc
      .parse_transaction({ boc: oldestTx })
      .then(response => response.parsed)
      .catch(error => console.log(error));

    const prevTransLt = parsedOldestTransaction.prev_trans_lt;

    const newTransactions = await getTransactionsList(
      account,
      Number(prevTransLt),
    );

    if (newTransactions.length === 0) {
      break;
    } else {
      transactions = transactions.concat(newTransactions);
    }
    console.log(transactions.length);
  }

  return transactions;
}

async function getTransactionsList(account, lt = Number.MAX_SAFE_INTEGER) {
  const request = {
    jsonrpc: "2.0",
    method: "getTransactionsList",
    params: {
      account: account,
      limit: 100,
      lastTransactionLt: lt,
    },
    id: 1,
  };
  const transactions = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  })
    .then(response => response.json())
    .then(response => response.result);
  return transactions;
}

await getAllTransactions(
  "0:a519f99bb5d6d51ef958ed24d337ad75a1c770885dcd42d51d6663f9fcdacfb2",
);

```

  </details>

### `getTransaction`

This method is used to retrieve a single transaction with a given transaction
ID. This method is needed in cases where the user wants to retrieve a specific
transaction and has its transaction ID.

To call this method, the following JSON object should be composed and sent to
the server using a POST request:

```
{
  "jsonrpc": "2.0",
  "method": "getTransaction",
  "params": {
    "id": <hex_encoded>
  },
  "id": <number>
}
```

`id` (string) - transaction ID hex-encoded byte array

The server will return a JSON-RPC response with the following format:

```
{
  "jsonrpc": "2.0",
  "result": <base64>,
  "id": <number>
}

```

  <details>
    <summary>Example (JavaScript)</summary>

```
const url = "https://jrpc.everwallet.net/rpc";

const request = {
  jsonrpc: "2.0",
  method: "getTransaction",
  params: {
    id: "7b5ad058f9ddba0ea4c60ccd185b4b061e2968bc371ec14f7fc41550ef81d146",
  },
  id: 1,
};

fetch(url, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(request),
})
  .then(response => response.json())
  .then(response => console.log(response));


```

  </details>

  <details>
    <summary>Example (Python)</summary>

```

import requests

response = requests.post(
  "https://jrpc.everwallet.net/rpc",
  json = {
    "jsonrpc": "2.0",
    "method": "getTransaction",
    "params": {
      "id": "7b5ad058f9ddba0ea4c60ccd185b4b061e2968bc371ec14f7fc41550ef81d146",
    },
    "id": 1
  },
);


result = response.json() print(result)

```

  </details>

  <details>
    <summary>Result</summary>

```

{jsonrpc: '2.0', result: 'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3C…n5pQUA8AMKVQ6UR93A0SYUx7BuorhquQAOkIAAAAOA==', id: 1}

```

  </details>

### `getDstTransaction`

This method is used to retrieve a transaction that was created after processing
the inbound message. The message hash parameter is a 32-byte array that uniquely
identifies the inbound message.

To call this method, the client should send a JSON-RPC request to the server
with the following parameters:

```

{
  jsonrpc: "2.0",
  method: "getDstTransaction",
  params: { "messageHash": <hex_encoded> },
  id: <number>,
}


```

The server will return a JSON-RPC response with the following format:

```

{
  "jsonrpc":"2.0",
  "result": <base64>,
  "id": <number>
}

```

  <details>
    <summary>Example (JavaScript)</summary>

```

import fetch from "node-fetch";

const url = "https://jrpc.everwallet.net/rpc";

const request = {
  jsonrpc: "2.0",
  method: "getDstTransaction",
  params: {
    messageHash:
      "2b1ea3e56bf4f48917cd77662b3bac25e726e5447c9e83444416429117faf588",
  },
  id: 1,
};

fetch(url, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(request),
})
  .then(response => response.json())
  .then(response => console.log(response));


```

  </details>

  <details>
    <summary>Example (Python):</summary>

```

import requests

response = requests.post(
  "https://jrpc.everwallet.net/rpc",
  json = {
    "jsonrpc": "2.0",
    "method": "getDstTransaction",
    "params": {
      "messageHash":
        "2b1ea3e56bf4f48917cd77662b3bac25e726e5447c9e83444416429117faf588",
    },
    "id": 1
  }
)

result = response.json() print(result)

```

  </details>

  <details>
    <summary>Result:</summary>

```

{jsonrpc: '2.0', result:'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3C…n5pQUA8AMKVQ6UR93A0SYUx7BuorhquQAOkIAAAAOA==', id: 1}

```

  </details>

### `sendMessage`

This method is used to send a message within the Everscale blockchain. The
message can be of type internal, external, or event, and includes a header with
information about the source and destination addresses and the type of message.
This method is typically called by a smart contract or non-blockchain
application to initiate communication with another smart contract or external
application.

To call the sendMessage method, you will need to compose a JSON-RPC request and
send it to the server. The JSON-RPC request should have the following structure:

```

{ "jsonrpc": "2.0",
  "method": "sendMessage",
  "params":
    {
      "message": {
          "header": <common_msg_info>,
          "init": Option<state_init>,
          "body": Option<slice_data>,
          "body_to_ref": Option<bool>,
          "nit_to_ref": Option<bool>, }
    }, "id": <number> }

```

`message` - should be an object containing the details of the message you want
to send. This includes the message header, which specifies the type of message
(`IntMsgInfo`, `ExtInMsgInfo`, or `ExtOutMsgInfo`) and the source and
destination addresses. The init and body parameters are optional and can be used
to specify additional data for the message. The last two fields `body_to_ref`
and `init_to_ref` are used only for serialization purpose.

For details see https://docs.everscale.network/arch/message/

The server will return a JSON-RPC response with the following format:

```

{
  "jsonrpc": "2.0",
  "result": "<base64>",
  "id": <number>
}

```

  <details>
    <summary>Example (JavaScript)</summary>

```
import fetch from "node-fetch";
import { TonClient } from "@eversdk/core";
import { libNode } from "@eversdk/lib-node";
TonClient.useBinaryLibrary(libNode);

const client = new TonClient({ network: { endpoints: ["net.ton.dev"] } });

const message = await TON.abi.encode_message({
  abi: { type: "Json", value: abi },
  address: address,
  call_set: { function_name: functionName, input: input },
  signer: { type: "None" },
});

const response = fetch("https://jrpc.everwallet.net/rpc", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    jsonrpc: "2.0",
    method: "sendMessage",
    params: { message: message.message },
    id: 1,
  }),
})
  .then(response => response.json())
  .then(response => console.log(response));

```

  </details>

  <details>
    <summary>Result</summary>

```
{jsonrpc: '2.0', result:
'te6ccgECEAEAAyUAA7d6UZ+Zu11tUe+VjtJNM3rXWhx3C…n5pQUA8AMKVQ6UR93A0SYUx7BuorhquQAOkIAAAAOA==',
id: 1}
```

  </details>
