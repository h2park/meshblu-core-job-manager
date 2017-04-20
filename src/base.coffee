_                = require 'lodash'
moment           = require 'moment'
When             = require 'when'
GenericPool      = require 'generic-pool'
{ EventEmitter } = require 'events'
Redis            = require 'ioredis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-job-manager:job-manager-base')

class JobManagerBase extends EventEmitter
  constructor: (options={}) ->
    {
      @namespace
      @redisUri
      @idleTimeoutMillis
      @maxConnections
      @minConnections
      @evictionRunIntervalMillis
      @acquireTimeoutMillis
      @jobTimeoutSeconds
      @queueTimeoutSeconds
    } = options
    @idleTimeoutMillis ?= 60000
    @evictionRunIntervalMillis ?= 60000
    @acquireTimeoutMillis ?= 5000
    @minConnections ?= 0
    @_heartbeat = moment()

    throw new Error 'JobManagerResponder constructor is missing "namespace"' unless @namespace?
    throw new Error 'JobManagerResponder constructor is missing "redisUri"' unless @redisUri?
    throw new Error 'JobManagerResponder constructor is missing "idleTimeoutMillis"' unless @idleTimeoutMillis?
    throw new Error 'JobManagerResponder constructor is missing "maxConnections"' unless @maxConnections?
    throw new Error 'JobManagerResponder constructor is missing "minConnections"' unless @minConnections?
    throw new Error 'JobManagerRequester constructor is missing "jobTimeoutSeconds"' unless @jobTimeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "queueTimeoutSeconds"' unless @queueTimeoutSeconds?

    @_queuePool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }
    @_commandPool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }
    @_pubSubPool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }

  addMetric: (metadata, metricName, callback) =>
    return callback() if _.isEmpty metadata.jobLogs
    metadata.metrics ?= {}
    metadata.metrics[metricName] = Date.now()
    callback()

  _closeClient: (client) =>
    client.on 'error', =>
      # silently deal with it

    try
      if client.disconnect?
        client.quit()
        client.disconnect false
        return

      client.end true
    catch

  _createRedisPool: ({ maxConnections, minConnections, idleTimeoutMillis, evictionRunIntervalMillis, acquireTimeoutMillis, namespace, redisUri }) =>
    factory =
      create: =>
        return When.promise (resolve, reject) =>
          conx = new Redis redisUri, dropBufferSupport: true
          client = new RedisNS namespace, conx
          rejectError = (error) =>
            return reject error

          client.once 'error', rejectError
          client.once 'ready', =>
            client.removeListener 'error', rejectError
            resolve client

      destroy: (client) =>
        return When.promise (resolve, reject) =>
          @_closeClient client, (error) =>
            return reject error if error?
            resolve()

      validate: (client) =>
        return When.promise (resolve) =>
          client.ping (error) =>
            return resolve false if error?
            resolve true

    options = {
      max: maxConnections
      min: minConnections
      testOnBorrow: true
      idleTimeoutMillis
      evictionRunIntervalMillis
      acquireTimeoutMillis
    }

    pool = GenericPool.createPool factory, options

    pool.on 'factoryCreateError', (error) =>
      @emit 'factoryCreateError', error

    return pool

  healthcheck: (callback) =>
    healthy = @_heartbeat.isAfter moment().subtract @jobTimeoutSeconds * 2, 'seconds'
    _.defer =>
      callback null, healthy

  _updateHeartbeat: =>
    @_heartbeat = moment()

module.exports = JobManagerBase
