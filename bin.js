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

const BlindPeerClient = require('blind-peering')
const LookupClient = require('autobase-discovery/client/lookup.js')
const goodbye = require('graceful-goodbye')

const DEFAULT_MIN = 3
const DEFAULT_LIMIT = 6
const DEFAULT_STORAGE = path.join(path.normalize(os.homedir()), '.blind-peering-cli-storage')
const DEFAULT_TIMEOUT_SEC = 15

const seedCmd = command(
  'seed',
  description('Request a blind peer to keep hypercores and hyperdrive available'),
  arg('<key>', 'Hypercore/Hyperdrive key to seed'),
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  flag('--drive', 'Set this flag to request to seed a hyperdrive (including its blobs core)'),
  flag(
    '--no-announce',
    'Set this flag to disable announcing, so the blind peers will download the core/drive but will not announce it to the swarm'
  ),
  flag('--core', 'Set this flag to request to seed a hypercore'),
  flag(
    '--blind-peer-key|-b [blindPeerKey]',
    'Key of a blind peer. Can only be set if no auto-disc-db is used.'
  ).multiple(),
  flag('--auto-disc-db |-a [autoDiscDb]', 'Key of the autobase-discovery database to use'),
  flag(
    '--service-name |-s [serviceName]',
    'Service name whose instances to contact for the seed requests (as stored in the autobase-discovery database)'
  ),
  flag(
    '--limit|-l [limit]',
    `Maximum amount of instances to send a request to. Default ${DEFAULT_LIMIT}.`
  ),
  flag(
    '--min|-m [min]',
    `Minimum amount of instances to send a request to (will error if fewer can be reached). Default ${DEFAULT_MIN}`
  ),
  flag(
    '--timeout|-t [timeout]',
    `Timeout (in seconds) when connecting to a blind peer. Default: ${DEFAULT_TIMEOUT_SEC} seconds`
  ),
  flag('--debug|-d', 'Enable debug logs'),
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
        logger.warn(
          'Defaulting to --drive (deprecated behaviour, please specify either --drive or --core)'
        )
      }
      const isDrive = flags.drive || !flags.core
      const blindPeerKeys = flags.blindPeerKey
        ? flags.blindPeerKey.map((b) => IdEnc.decode(b))
        : null

      const shouldAnnounce = flags.announce
      if (!shouldAnnounce) {
        logger.info('Running in no-announce mode')
      }

      if ((blindPeerKeys && autoDiscDb) || (!blindPeerKeys && !autoDiscDb)) {
        throw new Error('Must set exactly one of blind-peer-key and auto-disc-db')
      }

      const min = blindPeerKeys ? blindPeerKeys.length : parseInt(flags.min || DEFAULT_MIN)

      const limit = blindPeerKeys ? blindPeerKeys.length : parseInt(flags.limit || DEFAULT_LIMIT)
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
      if (blindPeerKeys) {
        for (const b of blindPeerKeys) blindPeers.push(b)
      } else {
        logger.info(
          `Requesting up to ${limit} instances from the '${serviceName}' service, using autobase-discovery database ${IdEnc.normalize(autoDiscDb)}`
        )
        client = new LookupClient(autoDiscDb, swarm, store.namespace('autodiscovery-lookup'))

        await client.ready()

        for await (const { publicKey } of await client.list(serviceName, { limit })) {
          blindPeers.push(publicKey)
        }
        logger.info(
          `Using blind peers:\n  -${blindPeers.map((p) => IdEnc.normalize(p)).join('\n  -')}`
        )

        if (blindPeers.length < min) {
          logger.error(
            `Found only ${blindPeers.length} peers, whereas a minimum of ${min} was specified`
          )
          process.exit(1)
        }
      }

      let seedCore = null
      let blobsCore = null
      if (isDrive) {
        // TODO: should ideally live in the blind peer (detecting when it's a hyperdrive)
        logger.info('Obtaining the blobs key...')
        const cores = await getDbAndBlobs(store, key, swarm)
        seedCore = cores[0]
        blobsCore = cores[1]
      } else {
        const core = store.get({ key })
        await core.ready()
        seedCore = core
      }

      let msg = `Requesting ${blindPeers.length} blind peers to seed core ${IdEnc.normalize(key)}`
      if (blobsCore) msg += ` and blobs core ${IdEnc.normalize(blobsCore.key)}`
      msg += ` (minimum successes: ${min})`
      logger.info(msg)

      const blindPeerClient = new BlindPeerClient(swarm, store, {
        coreMirrors: blindPeers,
        pick: 1
      })
      const seedProms = []

      for (const b of blindPeers) {
        const proms = []
        proms.push(blindPeerClient.addCore(seedCore.session(), b, { announce: shouldAnnounce }))
        if (blobsCore) {
          proms.push(blindPeerClient.addCore(blobsCore.session(), b, { announce: false }))
        }
        seedProms.push(Promise.all(proms))
      }

      const { resolve: resolveSeed, reject: rejectSeed, promise: seedProm } = rrp()
      seedTimeout = setTimeout(
        () =>
          rejectSeed(new Error(`Timeout: failed to request ${min} blind peers to seed the core`)),
        msTimeout
      )

      let nrResolved = 0
      let nrRejected = 0
      const processReject = () => {
        nrRejected++
        if (nrRejected > blindPeers.length - min)
          rejectSeed(new Error(`Failed to request seeding at least ${min} blind peers`))
      }

      for (let i = 0; i < seedProms.length; i++) {
        const prom = seedProms[i]
        const normBlindPeerKey = IdEnc.normalize(blindPeers[i])

        prom.then(
          (res) => {
            const dbRes = res[0][0]
            const blobsRes = res.length > 1 ? res[1][0] : null // not a drive

            if (done) return

            if (!dbRes) {
              processReject()
              logger.warn(`Could not request core from blind peer ${normBlindPeerKey}`)
              return
            }
            if (dbRes.announce === false) {
              logger.warn(
                `You are not a trusted peer for blind peer ${normBlindPeerKey} (announce was downgraded to false)`
              )
              processReject()
              return
            }
            if (isDrive && !blobsRes) {
              logger.warn(`Could not request blobs core from blind peer ${normBlindPeerKey}`)
              processReject()
              return
            }

            nrResolved++
            logger.info(
              `Successfully contacted seeder ${normBlindPeerKey} (successes: ${nrResolved}, failures: ${nrRejected})`
            )
            if (nrResolved >= min) {
              resolveSeed()
            }
          },
          (err) => {
            safetyCatch(err)
            if (!done)
              logger.info(
                `Failed to contact seeder ${normBlindPeerKey} (successes: ${nrResolved}, failures: ${nrRejected})`
              )
            if (debug)
              logger.warn(`Error while communicating with seeder ${normBlindPeerKey}: ${err.stack}`)
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
        if (blobsCore) msg += `with blobs core ${IdEnc.normalize(blobsCore.key)}`
        logger.info(msg)
      }

      goodbye.exit()
    } catch (e) {
      console.error(e.message)
      goodbye.exit() // TODO: a way to signal non-zero exit code
    }
  }
)

const deleteCmd = command(
  'delete',
  description('Request blind peers to delete a hyperdrive or hypercore'),
  arg('<key>', 'Hypercore/Hyperdrive key to delete'),
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  flag('--drive', 'Set this flag to request to delete a hyperdrive (including its blobs core)'),
  flag('--core', 'Set this flag to request to delete a hypercore'),
  flag('--blind-peer|b [blindPeer]', 'Key of a blind peer (specify 1 or more)').multiple(),
  flag('--debug|-d', 'Enable debug logs'),
  async function ({ args, flags }) {
    const key = IdEnc.decode(args.key)
    const { debug } = flags

    const logger = pino({
      level: debug ? 'debug' : 'info',
      transport: {
        target: 'pino-pretty'
      }
    })

    if (!flags.drive && !flags.core) {
      logger.error('You must specify either --drive or --core)')
      process.exit(1)
    }
    const isDrive = flags.drive
    if (!flags.blindPeer || flags.blindPeer.length === 0) {
      console.error('Must specify at least 1 --blind-peer')
      process.exit(1)
    }
    const blindPeers = flags.blindPeer.map((b) => IdEnc.decode(b))

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
    let client = null
    goodbye(async () => {
      if (!done) logger.info('Cancelling')
      logger.info(done ? 'Shutting down...' : 'Cancelling...')
      await swarm.destroy()
      if (client) await client.close()
      await store.close()
    })

    logger.info(`Using blind peers:\n  -${blindPeers.map((p) => IdEnc.normalize(p)).join('\n  -')}`)

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

    {
      let msg = `Requesting ${blindPeers.length} blind peers to delete core ${IdEnc.normalize(key)}`
      if (isDrive) msg += ` and blobs core ${IdEnc.normalize(cores[1].key)}`
      logger.info(msg)
    }

    client = new BlindPeerClient(swarm, store, { coreMirrors: blindPeers, pick: blindPeers.length })

    try {
      for (const c of cores) {
        const r = await client.deleteCore(c.key)
        if (debug)
          logger.debug(`Delete request completed for ${IdEnc.normalize(c.key)}. Existed? ${r}`)
      }
    } catch (e) {
      if (!e.cause) throw e
      logger.error(`Error while contacting the blind peers: ${e.cause.message}`)
      goodbye.exit()
    }

    {
      let msg = `Successfully requested to delete ${isDrive ? 'hyperdrive' : 'hypercore'} ${IdEnc.normalize(key)}`
      if (blobsKey) msg += `with blobs core ${IdEnc.normalize(blobsKey)}`
      logger.info(msg)
    }

    done = true
    goodbye.exit()
  }
)

const identityCmd = command(
  'identity',
  flag('--storage|-s [path]', `Storage path. Defaults to ${DEFAULT_STORAGE}`),
  description('Print your DHT public key'),
  async function ({ args, flags }) {
    const logger = console

    const storage = path.normalize(flags.storage || DEFAULT_STORAGE)

    logger.info(`Using storage ${storage}`)
    const { store, swarm } = await getStoreAndSwarm(storage)
    const ownKey = IdEnc.normalize(swarm.dht.defaultKeyPair.publicKey)
    logger.info(`Your DHT public key is: ${ownKey}`)
    logger.info(
      `To be able to send 'seed' requests to blind peers, ask them to add the '--trusted-peer ${ownKey}' flag when launching their blind peer instance`
    )

    await swarm.destroy()
    await store.close()
  }
)

async function getDbAndBlobs(store, key, swarm) {
  const drive = new Hyperdrive(store.namespace('drive'), key)
  await drive.ready()
  await new Promise((resolve) => {
    if (drive.blobs) resolve()
    else {
      drive.once('blobs', resolve)
      swarm.join(drive.discoveryKey)
      drive.getBlobs().catch(safetyCatch)
    }
  })

  return [drive.db.core, drive.blobs.core]
}

async function getStoreAndSwarm(storage) {
  const store = new Corestore(storage)
  await store.ready()

  // We need a consistent keypair across restarts, because we use
  // an allow-list at the blind-peer side (to which our key should be added)
  const keyPair = await store.createKeyPair('dht-client-identity')
  const swarm = new Hyperswarm({ dht: new HyperDHT({ keyPair }) })

  return { store, swarm }
}

const cmd = command('blind-peering', seedCmd, deleteCmd, identityCmd)
cmd.parse()
