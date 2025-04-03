# Blind Peering CLI

Command-line interface for blind-peer interactions.

# Install

```
npm i -g @holepunchto/blind-peering-cli
```

## Usage

Currently only supports requesting to seed hyperdrives.

### Contact Multiple Blind Peers

```
blind-peering seed <hypercore key to seed> --auto-disc-db <Database key of an autobase-discovery service> --service-name <service name of the blind peers>
```

### Contact One Blind Peer

```
blind-peering seed <hypercore key to seed> --blind-peer-key <blind peer RPC Key>
```
