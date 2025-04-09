#! /usr/bin/env node

const os = require('os')
const path = require('path')

const { command, flag, arg, description } = require('paparam')

const { seed, identity } = require('.')

const DEFAULT_MIN = 3
const DEFAULT_LIMIT = 6
const DEFAULT_STORAGE = path.join(path.normalize(os.homedir()), '.blind-peering-cli-storage')
const DEFAULT_TIMEOUT_SEC = 15

const seedCmd = command('seed',
  description('Request a blind peer to keep a hyperdrive available'),
  arg('<key>', 'Hypercore key to seed'),
  flag('--auto-disc-db |-a [autoDiscDb]', 'Key of the autobase-discovery database to use'),
  flag('--service-name |-s [serviceName]', 'Service name whose instances to contact for the seed requests (as stored in the autobase-discovery database)'),
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  flag('--limit|-l [limit]', `Maximum amount of instances to send a request to. Default ${DEFAULT_LIMIT}.`),
  flag('--min|-m [min]', `Minimum amount of instances to send a request to (will error if fewer can be reached). Default ${DEFAULT_MIN}`),
  flag('--timeout|-t [timeout]', `Timeout (in seconds) when connecting to a blind peer. Default: ${DEFAULT_TIMEOUT_SEC} seconds`),
  flag('--debug|-d', 'Enable debug logs'),
  flag('--blind-peer-key|b [blindPeerKey]', 'Key of a blind peer. Can only be set if no auto-disc-db is used.'),
  async function ({ args, flags }) {
    seed({ ...args, ...flags })
  }
)

const identityCmd = command('identity',
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  description('Print your DHT public key'),
  async function ({ args, flags }) {
    identity({ ...args, ...flags })
  }
)

const cmd = command('blind-peering', seedCmd, identityCmd)
cmd.parse()
