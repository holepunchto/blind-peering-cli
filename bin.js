#! /usr/bin/env node

const os = require('os')
const path = require('path')

const Hyperswarm = require('hyperswarm')
const IdEnc = require('hypercore-id-encoding')
const Corestore = require('corestore')
const HyperDHT = require('hyperdht')
const { command, flag, arg, description } = require('paparam')
const rrp = require('resolve-reject-promise')
const safetyCatch = require('safety-catch')
const Hyperdrive = require('hyperdrive')
const pino = require('pino')

const BlindPeerClient = require('@holepunchto/blind-peering')
const LookupClient = require('autobase-discovery/client/lookup.js')
const goodbye = require('graceful-goodbye')

const DEFAULT_MIN = 3
const DEFAULT_LIMIT = 6
const DEFAULT_STORAGE = path.join(path.normalize(os.homedir()), '.blind-peering-cli-storage')
const DEFAULT_TIMEOUT_SEC = 15

const seedCmd = command('seed',
  description('Request a blind peer to keep a hyperdrive available'),
  arg('<key>', 'Hypercore key to seed'),
  flag('--drive', 'Request to seed a hyperdrive (including its blobs core)'),
  flag('--core', 'Request to seed a hypercore'),
  flag('--auto-disc-db |-a [autoDiscDb]', 'Key of the autobase-discovery database to use'),
  flag('--service-name |-s [serviceName]', 'Service name whose instances to contact for the seed requests (as stored in the autobase-discovery database)'),
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  flag('--limit|-l [limit]', `Maximum amount of instances to send a request to. Default ${DEFAULT_LIMIT}.`),
  flag('--min|-m [min]', `Minimum amount of instances to send a request to (will error if fewer can be reached). Default ${DEFAULT_MIN}`),
  flag('--timeout|-t [timeout]', `Timeout (in seconds) when connecting to a blind peer. Default: ${DEFAULT_TIMEOUT_SEC} seconds`),
  flag('--debug|-d', 'Enable debug logs'),
  flag('--blind-peer-key|b [blindPeerKey]', 'Key of a blind peer. Can only be set if no auto-disc-db is used.'),
  async function ({ args, flags }) {
    try {
      const key = IdEnc.decode(args.key)
      const { autoDiscDb, serviceName, debug } = flags

      const logger = pino({
        level: 'info',
        transport: {
          target: 'pino-pretty'
        }
      })

      if (!flags.drive && !flags.core) {
        logger.warn('Defaulting to --drive (deprecated behaviour, please specify either --drive or --core)')
      }
      const isDrive = flags.drive || (!flags.core)
      const blindPeerKey = flags.blindPeerKey ? IdEnc.decode(flags.blindPeerKey) : null

      if ((blindPeerKey && autoDiscDb) || (!blindPeerKey && !autoDiscDb)) {
        throw new Error('Must set exactly one of blind-peer-key and auto-disc-db')
      }

      const min = blindPeerKey
        ? 1
        : parseInt(flags.min || DEFAULT_MIN)

      const limit = blindPeerKey
        ? 1
        : parseInt(flags.limit || DEFAULT_LIMIT)
      const msTimeout = parseInt(flags.timeout || DEFAULT_TIMEOUT_SEC) * 1000
      const storage = path.normalize(flags.storage || DEFAULT_STORAGE)

      logger.info(`Using storage ${storage}`)
      const { store, swarm } = await getStoreAndSwarm(storage)
      logger.info(`Using DHT public key: ${IdEnc.normalize(swarm.dht.defaultKeyPair.publicKey)}`)

      swarm.on('connection', (conn, peerInfo) => {
        if (debug) {
          const key = IdEnc.normalize(peerInfo.publicKey)
          logger.debug(`Opened connection to ${key}`)
          conn.on('close', () => logger.debug(`Closed connection to ${key}`))
        }
        store.replicate(conn)
      })

      let done = false
      let seedTimeout = null
      let client = null
      goodbye(async () => {
        clearTimeout(seedTimeout)
        logger.info(done ? 'Shutting down...' : 'Cancelling...')
        await swarm.destroy()
        if (client) await client.close()
        await store.close()
      })

      const blindPeers = []
      if (blindPeerKey) {
        blindPeers.push(blindPeerKey)
      } else {
        logger.info(`Requesting up to ${limit} instances from the '${serviceName}' service, using autobase-discovery database ${IdEnc.normalize(autoDiscDb)}`)
        client = new LookupClient(
          autoDiscDb, swarm, store.namespace('autodiscovery-lookup')
        )

        await client.ready()

        for await (const { publicKey } of await client.list(serviceName, { limit })) {
          blindPeers.push(publicKey)
        }
        logger.info(`Using blind peers:\n  -${(blindPeers.map(p => IdEnc.normalize(p)).join('\n  -'))}`)

        if (blindPeers.length < min) {
          logger.error(`Found only ${blindPeers.length} peers, whereas a minimum of ${min} was specified`)
          process.exit(1)
        }
      }

      const cores = []
      let blobsKey = null
      if (isDrive) {
        // TODO: should ideally live in the blind peer (detecting when it's a hyperdrive)
        logger.info('Obtaining the blobs key...')
        const [dbCore, blobsCore] = await getDbAndBlobs(store, key, swarm)
        blobsKey = blobsCore.key
        cores.push(dbCore)
        cores.push(blobsCore)
      } else {
        const core = store.get({ key })
        await core.ready()
        cores.push(core)
      }

      let msg = `Requesting ${blindPeers.length} blind peers to seed core ${IdEnc.normalize(key)}`
      if (isDrive) msg += ` and blobs core ${IdEnc.normalize(cores[1].key)}`
      msg += ` (minimum successes: ${min})`
      logger.info(msg)

      const blindPeerClient = new BlindPeerClient(swarm, store, { coreMirrors: blindPeers })
      const seedProms = []
      for (const b of blindPeers) {
        const proms = []
        for (const c of cores) {
          proms.push(blindPeerClient.addCore(c.session(), b, { announce: true }))
        }
        seedProms.push(Promise.all(proms))
      }

      const { resolve: resolveSeed, reject: rejectSeed, promise: seedProm } = rrp()
      seedTimeout = setTimeout(
        () => rejectSeed(new Error(`Timeout: failed to request ${min} blind peers to seed the core`)),
        msTimeout
      )

      let nrResolved = 0
      let nrRejected = 0
      const processReject = () => {
        nrRejected++
        if (nrRejected > blindPeers.length - min) rejectSeed(new Error(`Failed to request seeding at least ${min} blind peers`))
      }

      for (let i = 0; i < seedProms.length; i++) {
        const prom = seedProms[i]
        const normBlindPeerKey = IdEnc.normalize(blindPeers[i])

        prom.then(
          ([dbRes, blobsRes]) => {
            if (done) return
            if (!dbRes) {
              processReject()
              logger.warn(`Could not request core from blind peer ${normBlindPeerKey}`)
              return
            }
            if (dbRes.announce === false) {
              logger.warn(`You are not a trusted peer for blind peer ${normBlindPeerKey} (announce was downgraded to false)`)
              processReject()
              return
            }
            if (isDrive && !blobsRes) {
              logger.warn(`Could not request blobs core from blind peer ${normBlindPeerKey}`)
              processReject()
              return
            }

            nrResolved++
            logger.info(`Successfully contacted seeder ${normBlindPeerKey} (successes: ${nrResolved}, failures: ${nrRejected})`)
            if (nrResolved >= min) {
              resolveSeed()
            }
          },
          (err) => {
            safetyCatch(err)
            if (!done) logger.info(`Failed to contact seeder ${normBlindPeerKey} (successes: ${nrResolved}, failures: ${nrRejected})`)
            if (debug) logger.warn(`Error while communicating with seeder ${normBlindPeerKey}: ${err.stack}`)
            processReject()
          }
        )
      }

      try {
        await seedProm
      } catch (e) {
        if (debug) {
          logger.error(e)
        } else {
          logger.error(e.message)
        }
        goodbye.exit() // TODO: a way to signal non-zero exit code
        return
      }
      done = true

      {
        // TODO: verify it was announced + verify it is downloaded
        let msg = `Successfully requested to seed ${isDrive ? 'hyperdrive' : 'hypercore'} ${IdEnc.normalize(key)}`
        if (blobsKey) msg += `with blobs core ${IdEnc.normalize(blobsKey)}`
        logger.info(msg)
      }

      goodbye.exit()
    } catch (e) {
      console.error(e.message)
      goodbye.exit() // TODO: a way to signal non-zero exit code
    }
  }
)

const identityCmd = command('identity',
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  description('Print your DHT public key'),
  async function ({ args, flags }) {
    const logger = console

    const storage = path.normalize(flags.storage || DEFAULT_STORAGE)

    logger.info(`Using storage ${storage}`)
    const { store, swarm } = await getStoreAndSwarm(storage)
    const ownKey = IdEnc.normalize(swarm.dht.defaultKeyPair.publicKey)
    logger.info(`Your DHT public key is: ${ownKey}`)
    logger.info(`To be able to send 'seed' requests to blind peers, ask them to add the '--trusted-peer ${ownKey}' flag when launching their blind peer instance`)

    await swarm.destroy()
    await store.close()
  }
)

async function getDbAndBlobs (store, key, swarm) {
  const drive = new Hyperdrive(store.namespace('drive'), key)
  await drive.ready()
  await new Promise(resolve => {
    if (drive.blobs) resolve()
    else {
      drive.once('blobs', resolve)
      swarm.join(drive.discoveryKey)
      drive.getBlobs().catch(safetyCatch)
    }
  })

  return [drive.db.core, drive.blobs.core]
}

async function getStoreAndSwarm (storage) {
  const store = new Corestore(storage)
  await store.ready()

  // We need a consistent keypair across restarts, because we use
  // an allow-list at the blind-peer side (to which our key should be added)
  const keyPair = await store.createKeyPair('dht-client-identity')
  const swarm = new Hyperswarm({ dht: new HyperDHT({ keyPair }) })

  return { store, swarm }
}

const cmd = command('blind-peering', seedCmd, identityCmd)
cmd.parse()
