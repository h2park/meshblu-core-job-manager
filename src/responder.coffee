_              = require 'lodash'
async          = require 'async'
JobManagerBase = require './base'
debug          = require('debug')('meshblu-core-job-manager:responder')

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

  enqueueJobs: =>
    async.doWhilst @enqueueJob, (=> @_allowProcessing)

  enqueueJob: (callback=_.noop) =>
    return _.defer callback if @queue.length() > @queue.concurrency

    @_enqueuing = true
    @dequeueJob (error, key) =>
      # order is important here
      @_enqueuing = false
      return callback() if error?
      return callback() if _.isEmpty key
      @_drained = false
      @queue.push key
      callback()

  _work: (key, callback) =>
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
            callback()

  dequeueJob: (callback) =>
    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpop @requestQueueName, @queueTimeoutSeconds, (error, result) =>
        @_updateHeartbeat()
        @_queuePool.release queueClient
        return callback error if error?
        return callback new Error 'No Result' unless result?

        [ channel, key ] = result
        return callback null, key
    .catch callback
    return

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
    @_allowProcessing = true
    @_stoppedProcessing = false
    @_drained = true
    @_enqueuing = false
    @queue.drain = =>
      debug 'drained'
      @_drained = true
    @enqueueJobs()
    _.defer callback

  _waitForStopped: (callback) =>
    _.delay callback, 100

  _safeToStop: =>
    @_allowProcessing == false && @_drained && @_enqueuing == false

  _stopProcessing: (callback) =>
    @_allowProcessing = false
    async.doUntil @_waitForStopped, @_safeToStop, callback

  stop: (callback=_.noop) =>
    @_stopProcessing callback

module.exports = JobManagerResponder
