const os = require('os')
const path = require('path')

const Hyperswarm = require('hyperswarm')
const IdEnc = require('hypercore-id-encoding')
const Corestore = require('corestore')
const HyperDHT = require('hyperdht')
const { command, flag, arg } = require('paparam')
const rrp = require('resolve-reject-promise')

const BlindPeerClient = require('@holepunchto/blind-peering')
const LookupClient = require('autobase-discovery/client/lookup.js')
const goodbye = require('graceful-goodbye')

const DEFAULT_MIN = 3
const DEFAULT_LIMIT = 6
console.info(path.join('/home/hans', '.blind-peering-cli-storage'))
const DEFAULT_STORAGE = path.join(path.normalize(os.homedir()), '.blind-peering-cli-storage')
const DEFAULT_TIMEOUT_SEC = 15

const seedCmd = command('seed',
  arg('<key>', 'Hypercore key to seed'),
  flag('--auto-disc-db |-a [autoDiscDb]', 'Key of the autobase-discovery database to use'),
  flag('--service-name |-s [serviceName]', 'Service name whose instances to contact for the seed requests (as stored in the autobase-discovery database)'),
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  flag('--limit|-l [limit]', `Maximum amount of instances to send a request to. Default ${DEFAULT_LIMIT}.`),
  flag('--min|-m [min]', `Minimum amount of instances to send a request to (will error if fewer can be reached). Default ${DEFAULT_MIN}`),
  flag('--timeout|-t [timeout]', `Timeout (in seconds) when connecting to a blind peer. Default: ${DEFAULT_TIMEOUT_SEC} seconds`),
  flag('--debug|-d', 'Enable debug logs'),
  async function ({ args, flags }) {
    const key = IdEnc.decode(args.key)
    const { autoDiscDb, serviceName, debug } = flags
    const min = parseInt(flags.min || DEFAULT_MIN)
    const limit = parseInt(flags.limit || DEFAULT_LIMIT)
    const msTimeout = parseInt(flags.timeout || DEFAULT_TIMEOUT_SEC) * 1000
    const storage = path.normalize(flags.storage || DEFAULT_STORAGE)

    console.info(`Using storage ${storage}`)
    const store = new Corestore(storage)
    await store.ready()

    // We need a consistent keypair across restarts, because we use
    // an allow-list at the blind-peer side (to which our key should be added)
    const keyPair = await store.createKeyPair('dht-client-identity')
    const swarm = new Hyperswarm({ dht: new HyperDHT({ keyPair }) })
    console.info(`Using DHT public key: ${IdEnc.normalize(swarm.dht.defaultKeyPair.publicKey)}`)

    swarm.on('connection', (conn, peerInfo) => {
      if (debug) {
        const key = IdEnc.normalize(peerInfo.publicKey)
        console.debug(`Opened connection to ${key}`)
        conn.on('close', () => console.debug(`Closed connection to ${key}`))
      }
      store.replicate(conn)
    })

    console.info(`Requesting up to ${limit} instances from the '${serviceName}' service, using autobase-discovery datbase ${IdEnc.normalize(autoDiscDb)}`)
    const client = new LookupClient(
      autoDiscDb, swarm, store.namespace('autodiscovery-lookup')
    )

    let done = false
    let seedTimeout = null
    goodbye(async () => {
      clearTimeout(seedTimeout)
      console.info(done ? 'Shutting down...' : 'Cancelling...')
      await swarm.destroy()
      await client.close()
      await store.close()
    })

    await client.ready()

    const blindPeers = []
    for await (const { publicKey } of await client.list(serviceName, { limit })) {
      blindPeers.push(publicKey)
    }
    console.info(`Using blind-peers:\n  -${(blindPeers.map(p => IdEnc.normalize(p)).join('\n  -'))}`)

    if (blindPeers.length < min) {
      console.error(`Found only ${blindPeers.length} peers, whereas a minimum of ${min} was specified`)
      process.exit(1)
    }

    console.info(`Requesting ${blindPeers.length} blind peers to seed core ${IdEnc.normalize(key)}`)

    const blindPeerClient = new BlindPeerClient(swarm, store, { coreMirrors: blindPeers })
    const seedProms = blindPeers.map(async b => await blindPeerClient.addCore(key, b, { announce: true }))
    const { resolve: resolveSeed, reject: rejectSeed, promise: seedProm } = rrp()
    seedTimeout = setTimeout(
      () => rejectSeed(new Error(`Timeout: failed to request ${min} blind peers to seed the core`)),
      msTimeout
    )

    let nrResolved = 0
    let nrRejected = 0
    const processReject = () => {
      nrRejected++
      if (nrRejected > blindPeers.length - min) rejectSeed(new Error(`Failed to request seeding at at least ${min} blind peers`))
    }

    for (const prom of seedProms) {
      prom.then(
        (res) => {
          if (res.announce !== true) {
            console.warn('You are not a trusted peer for the seeder (announce was downgraded to false)')
            processReject()
            return
          }

          console.log('res', res)
          nrResolved++
          console.info(`Successfully contacted seeder (successes: ${nrResolved}, failures: ${nrRejected})`)
          if (nrResolved >= min) {
            resolveSeed()
          }
        },
        (err) => {
          console.info(`Failed to contact seeder (successes: ${nrResolved}, failures: ${nrRejected})`)
          if (debug) console.warn(`Error while communicating with seeder: ${err.stack}`)
          processReject()
        }
      )
    }

    await seedProm
    done = true

    // TODO: verify it was announced + verify it is downloaded
    console.info('Successfully requested to seed the core')
    goodbye.exit()
  }
)

const cmd = command('blind-peering', seedCmd)
cmd.parse()
