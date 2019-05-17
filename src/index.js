const moment = require('moment')
const { createDbPool } = require('@hotelflex/db-utils')

const OPS_TABLE = 'operations'
const SCAN_INTERVAL = 60000

class OpCleaner {
  constructor() {
    this.isRunning = false
    this.start = this.start.bind(this)
    this.scanForOldOps = this.scanForOldOps.bind(this)
  }

  async start(config, logger) {
    this.postgres = config.postgres
    this.deleteAfter = config.deleteAfter
    this.logger = logger || console
    try {
      this.logger.info('OpCleaner: starting up')
      this.db = createDbPool(this.postgres, { min: 2, max: 8 })
      setInterval(this.scanForOldOps, SCAN_INTERVAL)
      this.scanForOldOps()
    } catch (e) {
      this.logger.error({ error: e.message }, 'OpCleaner: Error starting up')
      process.exit()
    }
  }

  async scanForOldOps() {
    if(this.isRunning) return
    try {
      this.isRunning = true
      const deleteAfter = this.deleteAfter || 24
      const timestamp = moment
        .utc()
        .subtract(deleteAfter, 'hours')
        .format('YYYY-MM-DDTHH:mm:ss')
  

      const count = await this.db
        .from(OPS_TABLE)
        .where('committed', true)
        .where('timestamp', '<=', timestamp)
        .limit(50000)
        .del()

      this.logger.debug(`OpCleaner: Deleted ${count} operations`)
    } catch(error) {
      this.logger.error(
        { error: error.message },
        'OpCleaner: Error deleting operations'
      )
    } finally {
      this.isRunning = false
    }
  }
}

module.exports = new OpCleaner()
