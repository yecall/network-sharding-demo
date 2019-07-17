# Network sharding demo

YeeCo is a full sharding block chain system.

Each shard runs in independent subnetwork.

This demo shows how YeeCo implements network sharding.

## Install
```
$ cargo build
```

## Usage

Now we will run a demo with 2 shards.

### 1. Start bootnodes

shard 0
```
$ target/debug/network-sharding-demo --node-key=0000000000000000000000000000000000000000000000000000000000000001 --port=60001 --foreign-port=61001 --shard-num=0
```

shard 1
```
$ target/debug/network-sharding-demo --node-key=0000000000000000000000000000000000000000000000000000000000000002 --port=60011 --foreign-port=61011 --shard-num=1

```

### 2. Start bootnodes router

Create a toml file on /tmp/network-sharding-demo/conf/bootnodes-router.toml 

File content:
```toml
[shards]
[shards.0]
native = ["/ip4/127.0.0.1/tcp/60001/p2p/QmQZ8TjTqeDj3ciwr93EJ95hxfDsb9pEYDizUAbWpigtQN"]
foreign = ["/ip4/127.0.0.1/tcp/61001/p2p/QmQZ8TjTqeDj3ciwr93EJ95hxfDsb9pEYDizUAbWpigtQN"]

[shards.1]
native = ["/ip4/127.0.0.1/tcp/60011/p2p/QmXiB3jqqn2rpiKU7k1h7NJYeBg8WNSx9DiTRKz9ti2KSK"]
foreign = ["/ip4/127.0.0.1/tcp/61011/p2p/QmXiB3jqqn2rpiKU7k1h7NJYeBg8WNSx9DiTRKz9ti2KSK"]
```

Start bootnodes router
```
$ target/debug/network-sharding-demo bootnodes-router --base-path=/tmp/network-sharding-demo

```

You can check if bootnodes router works well
```shell
$ curl -X POST --data '{"jsonrpc":"2.0","method":"bootnodes","params":[],"id":1}' localhost:50001 -H 'Content-Type: application/json'
```

### 3. Start nodes

shard 0
```
$ target/debug/network-sharding-demo --bootnodes-router=http://localhost:50001 --shard-num=0 --port=60002 --foreign-port=61002
```

shard 1
```
$ target/debug/network-sharding-demo --bootnodes-router=http://localhost:50001 --shard-num=1 --port=60012 --foreign-port=61012
```

### 4. Scenarios
### 4.1 Transaction in shard
**Operation:**

Input ***"Transaction(from: 0, to: 0)"*** in the console of the node of shard 0 (the one started with --port=60002),
then 

**Result:**

The node of shard 0 (the one started with --port=60001) printed ***"!!! Got a transaction: Transaction { from: 0, to: 0 }"***, while those of shard 1 did not print.

### 4.2 Transaction across shards
**Operation:**

Input ***"Transaction(from: 0, to: 1)"*** in the console of the node of shard 0 (the one started with --port=60002),
then 

**Result:**

Both of the node of shard 0 (the one started with --port=60001) and the nodes of shard 1 (the one started with --port=60002 or --port=60012) printed ***"!!! Got a transaction: Transaction { from: 0, to: 1 }"***.

### 4.3 Block head syncing across shards
**Operation:**

Input ***"Block(shard_num: 0, height: 100)"*** in the console of the node of shard 0 (the one started with --port=60002),
then 

**Result:**

The node of shard 0 (the one started with --port=60001)  printed ***"!!! Got a block: Block { shard_num: 0, height: 100 }"***, while the nodes of shard 1 (the one started with --port=60002 or --port=60012) printed ***"!!! Got a block head: BlockHead { shard_num: 0, height: 100 }"***.