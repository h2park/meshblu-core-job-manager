_                 = require 'lodash'
async             = require 'async'
JobManagerBase    = require './base'
debug             = require('debug')('meshblu-core-job-manager:responder')
debugSaturation   = require('debug')('meshblu-core-job-manager:responder:saturation')
SimpleBenchmark   = require 'simple-benchmark'
ResponderDequeuer = require './responder-dequeuer'

class JobManagerResponder extends JobManagerBase
  constructor: (options={}) ->
    {
      @requestQueueName
      @workerFunc
      concurrency=1
    } = options

    throw new Error 'JobManagerResponder constructor is missing "requestQueueName"' unless @requestQueueName?
    throw new Error 'JobManagerResponder constructor is missing "workerFunc"' unless @workerFunc?

    {concurrency=1} = options
    @queue = async.queue @_work, concurrency
    @queue.empty = => @emit 'empty'
    super
    @dequeuers = [
      new ResponderDequeuer { @queue, @_queuePool, @_updateHeartbeat, @requestQueueName, @queueTimeoutSeconds }
      new ResponderDequeuer { @queue, @_queuePool, @_updateHeartbeat, @requestQueueName, @queueTimeoutSeconds }
    ]

  createResponse: ({responseId, response}, callback) =>
    { metadata, data, rawData } = response
    metadata.responseId ?= responseId
    data ?= null
    rawData ?= JSON.stringify data

    @client.hmget metadata.responseId, ['request:metadata', 'response:queueName'], (error, result) =>
      delete error.code if error?
      return callback error if error?

      [ requestMetadata, responseQueueName ] = result ? []

      return callback() if _.isEmpty(requestMetadata) and _.isEmpty(responseQueueName)
      return callback new Error 'missing responseQueueName' unless responseQueueName?

      try
        requestMetadata = JSON.parse requestMetadata
      catch

      requestMetadata ?= {}

      if requestMetadata.ignoreResponse
        @client.del metadata.responseId, (error) =>
          delete error.code if error?
          return callback error if error?
          return callback null, {metadata, rawData}
        return

      metadata.jobLogs = requestMetadata.jobLogs if requestMetadata.jobLogs?
      metadata.metrics = requestMetadata.metrics if requestMetadata.metrics?

      @addMetric metadata, 'enqueueResponseAt', (error) =>
        return callback error if error?

        async.series [
          async.apply @client.publish, responseQueueName, JSON.stringify({ metadata, rawData })
          async.apply @client.expire, metadata.responseId, @jobTimeoutSeconds
        ], (error) =>
          delete error.code if error?
          callback null, { metadata, rawData }

    return # avoid returning redis

  _work: (key, callback) =>
    benchmark = new SimpleBenchmark label: '_work'
    @getRequest key, (error, job) =>
      return callback error if error?
      process.nextTick =>
        @workerFunc job, (error, response) =>
          if error?
            console.error error.stack
            callback()
          responseId = _.get job, 'metadata.responseId'
          @createResponse {responseId, response}, (error) =>
            console.error error.stack if error?
            debug benchmark.toString()
            callback()

  getRequest: (key, callback) =>
    @client.hgetall key, (error, result) =>
      delete error.code if error?
      return callback error if error?
      return callback new Error 'Missing result' if _.isEmpty result
      return callback new Error 'Missing metadata' if _.isEmpty result['request:metadata']

      metadata = JSON.parse result['request:metadata']
      @addMetric metadata, 'dequeueRequestAt', (error) =>
        return callback error if error?

        request =
          createdAt: result['request:createdAt']
          metadata:  metadata
          rawData:   result['request:data']

        @client.hset key, 'request:metadata', JSON.stringify(metadata), (error) =>
          return callback error if error?
          callback null, request

  start: (callback=_.noop) =>
    @_commandPool.acquire().then (@client) =>
      @client.once 'error', (error) =>
        @emit 'error', error

      @_startProcessing callback
    .catch callback
    return # promises

  _startProcessing: (callback) =>
    @_drained = true
    @queue.drain = =>
      debug 'drained'
      @_drained = true
    tasks = []
    _.each @dequeuers, (dequeuer) =>
      tasks.push dequeuer.start
    async.parallel tasks, callback

  _waitForStopped: (callback) =>
    _.delay callback, 100

  stop: (callback=_.noop) =>
    tasks = []
    _.each @dequeuers, (dequeuer) =>
      tasks.push dequeuer.stop
    async.parallel tasks, =>
      async.doUntil @_waitForStopped, (=> @_drained), callback

module.exports = JobManagerResponder
