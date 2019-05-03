const _ = require('lodash')
const moment = require('moment')
const rabbit = require('rabbot')
const EventEmitter = require('events').EventEmitter
const { createDbPool, createDbConn } = require('@hotelflex/db-utils')
const { postgres, rabbitmq } = require('../config')
const logger = require('../logger')

const OPS_TABLE = 'operations'
const SCAN_INTERVAL = 2000
const DELETE_INTERVAL = 60000

const db = createDbPool(postgres, { min: 2, max: 8 })
const pubSubConn = createDbConn(postgres)

rabbit.on('closed', () => {
  logger.error('RabbitMQ connection closed')
  process.exit()
})
rabbit.on('unreachable', () => {
  logger.error('RabbitMQ connection unreachable')
  process.exit()
})
rabbit.on('failed', () => {
  logger.error('RabbitMQ failure')
  process.exit()
})

const connectToBroker = () =>
  rabbit.configure({
    connection: Object.assign(
      { name: 'default', replyQueue: false },
      _.omit(rabbitmq, 'exchange'),
    ),
    exchanges: [
      {
        name: rabbitmq.exchange,
        type: 'topic',
        durable: true,
        persistent: true,
        publishTimeout: 2000,
      },
    ],
  })

class Daemon {
  constructor() {
    this.opIdMap = {}
    this.opEmitter = new EventEmitter()
    this.scanForUncommittedOps = this.scanForUncommittedOps.bind(this)
    this.scanForOldOps = this.scanForOldOps.bind(this)
    this.commitOp = this.commitOp.bind(this)
    this.publish = this.publish.bind(this)
    this.listenForInserts = this.listenForInserts.bind(this)
  }

  async start() {
    try {
      logger.info('OpTail: starting up')
      await connectToBroker()
      this.opEmitter.on('op', this.commitOp)
      await this.listenForInserts()
      setInterval(this.scanForUncommittedOps, SCAN_INTERVAL)
      setInterval(this.scanForOldOps, DELETE_INTERVAL)
    } catch (error) {
      console.log(error)
      process.exit()
    }
  }

  async scanForUncommittedOps() {
    const limit = Math.max(40 - Object.keys(this.opIdMap).length, 0)

    const ops = await db
      .select('id', 'messages')
      .from(OPS_TABLE)
      .where('committed', false)
      .limit(limit)

    ops.forEach(op => {
      if (this.opIdMap[op.id]) return

      this.opIdMap[op.id] = true
      this.opEmitter.emit('op', op)
    })
  }

  async scanForOldOps() {
    const oneDayAgo = moment
      .utc()
      .subtract(1, 'day')
      .format('YYYY-MM-DDYHH:mm:ss')

    const count = await db
      .from(OPS_TABLE)
      .where('committed', true)
      .where('timestamp', '<=', oneDayAgo)
      .del()

    logger.debug(`Deleted ${count} operations`)
  }

  async commitOp(op) {
    try {
      const messages = op.messages ? JSON.parse(op.messages) : []
      if (messages.length > 0) {
        logger.info(
          {
            operationId: op.id,
            messages,
          },
          'publishing messages',
        )
        await this.bulkPublish(messages)
      }

      await db(OPS_TABLE)
        .where('id', op.id)
        .update({ committed: true, messages: null })

      delete this.opIdMap[op.id]
    } catch (error) {
      console.log(error)
      process.exit()
    }
  }

  bulkPublish(msgs) {
    return rabbit.bulkPublish({
      [rabbitmq.exchange]: msgs.map(m => ({
        messageId: m.id,
        type: m.topic,
        headers: {
          'transaction-id': m.transactionId,
          'operation-id': m.operationId,
        },
        timestamp: moment(m.timestamp).unix(),
        body: m.body,
      })),
    })
  }

  publish(msg) {
    return rabbit.publish(rabbitmq.exchange, {
      messageId: msg.id,
      type: msg.topic,
      headers: {
        'transaction-id': msg.transactionId,
        'operation-id': msg.operationId,
      },
      timestamp: moment(msg.timestamp).unix(),
      body: msg.body,
    })
  }

  async listenForInserts() {
    await pubSubConn.query('LISTEN operations_insert')
    pubSubConn.on('notification', async ({ payload }) => {
      const _ops = await db
        .select()
        .from(OPS_TABLE)
        .where('id', payload)
      const op = _ops[0]

      if (this.opIdMap[op.id] || op.committed) return

      this.opIdMap[op.id] = true
      this.opEmitter.emit('op', op)
    })
  }
}

module.exports = new Daemon()
