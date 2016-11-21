_                = require 'lodash'
{ Pool }         = require '@octoblu/generic-pool'
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
    } = options
    @idleTimeoutMillis ?= 60000
    @minConnections ?= 0

    throw new Error 'JobManagerResponder constructor is missing "namespace"' unless @namespace?
    throw new Error 'JobManagerResponder constructor is missing "redisUri"' unless @redisUri?
    throw new Error 'JobManagerResponder constructor is missing "idleTimeoutMillis"' unless @idleTimeoutMillis?
    throw new Error 'JobManagerResponder constructor is missing "maxConnections"' unless @maxConnections?
    throw new Error 'JobManagerResponder constructor is missing "minConnections"' unless @minConnections?

    @_queuePool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }
    @_commandPool = @_createRedisPool { @maxConnections, @minConnections, @idleTimeoutMillis, @namespace, @redisUri }

  addMetric: (metadata, metricName, callback) =>
    return callback() unless _.isArray metadata.jobLogs
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

  _createRedisPool: ({ maxConnections, minConnections, idleTimeoutMillis, namespace, redisUri }) =>
    return new Pool
      max: maxConnections
      min: minConnections
      idleTimeoutMillis: idleTimeoutMillis
      create: (callback) =>
        conx = new Redis redisUri, dropBufferSupport: true
        client = new RedisNS namespace, conx
        client.ping (error) =>
          return callback error if error?
          client.once 'error', (error) =>
            @_closeClient client

          callback null, client

      destroy: @_closeClient

      validateAsync: (client, callback) =>
        client.ping (error) =>
          return callback false if error?
          callback true

module.exports = JobManagerBase
