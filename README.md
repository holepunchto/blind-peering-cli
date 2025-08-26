# Blind Peering CLI

Command-line interface for blind-peer interactions.

# Install

```
npm i -g blind-peering-cli
```

## Usage

Supports seeding hyperdrives (`--drive`) or hypercores (`--core`).

Note: only trusted peers can request to seed. To make yourself a trusted peer, pass the DHT public key output by `blind-peering identity` to the blind-peer admin (who will set the `--trusted-peer` flag with your key).

### Contact One Blind Peer

```
blind-peering seed --core --blind-peer-key <blind peer RPC Key> <hypercore key to seed>
```

### Contact Multiple Blind Peers

```
blind-peering seed --core --auto-disc-db <Database key of an autobase-discovery service> --service-name <service name of the blind peers> <hypercore key to seed>
```
