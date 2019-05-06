const moment = require('moment')
const { createDbPool } = require('@hotelflex/db-utils')

const OPS_TABLE = 'operations'
const SCAN_INTERVAL = 60000

class OpCleaner {
  constructor() {
    this.start = this.start.bind(this)
    this.scanForOldOps = this.scanForOldOps.bind(this)
  }

  async start(config, logger) {
    this.postgres = config.postgres
    this.logger = logger || console
    try {
      this.logger.info('OpCleaner: starting up')
      this.db = createDbPool(this.postgres, { min: 2, max: 8 })
      setInterval(this.scanForOldOps, SCAN_INTERVAL)
    } catch (e) {
      this.logger.error({ error: e.message }, 'OpCleaner: Error starting up')
      process.exit()
    }
  }

  async scanForOldOps() {
    try {
      const oneDayAgo = moment
        .utc()
        .subtract(1, 'day')
        .format('YYYY-MM-DDYHH:mm:ss')

      const count = await this.db
        .from(OPS_TABLE)
        .where('committed', true)
        .where('timestamp', '<=', oneDayAgo)
        .limit(50000)
        .del()

      this.logger.debug(`OpCleaner: Deleted ${count} operations`)
    } catch(e) {
      this.logger.error(
        { error: error.message },
        'OpCleaner: Error deleting operations'
      )
    }
  }
}

module.exports = new OpCleaner()
