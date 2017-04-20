_              = require 'lodash'
async          = require 'async'
JobManagerBase = require './base'

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

  createResponse: (options, callback) =>
    { metadata, data, rawData } = options
    { responseId } = metadata
    data ?= null
    rawData ?= JSON.stringify data

    @client.hmget responseId, ['request:metadata', 'response:queueName'], (error, result) =>
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
        @client.del responseId, (error) =>
          delete error.code if error?
          return callback error if error?
          return callback null, {metadata, rawData}
        return

      metadata.jobLogs = requestMetadata.jobLogs if requestMetadata.jobLogs?
      metadata.metrics = requestMetadata.metrics if requestMetadata.metrics?

      @addMetric metadata, 'enqueueResponseAt', (error) =>
        return callback error if error?

        metadataStr = JSON.stringify metadata

        values = [
          'response:metadata', metadataStr
          'response:data', rawData
        ]

        async.series [
          async.apply @client.hmset, responseId, values
          async.apply @client.lpush, responseQueueName, responseId
          async.apply @client.expire, responseId, @jobTimeoutSeconds
        ], (error) =>
          delete error.code if error?
          callback null, { metadata, rawData }

    return # avoid returning redis

  enqueueJob: (callback=_.noop) =>
    return callback() unless @_allowProcessing

    @_enqueuing = true
    async.retry @getRequest, (error, job) =>
      # order is important here
      @_drained = false
      @_enqueuing = true
      return callback() if error?
      return callback() if _.isEmpty job
      @queue.push job, callback

  _work: (job, callback) =>
    @workerFunc job, (error, response) =>
      if error?
        console.error error.stack
        callback()
      @createResponse response, (error) =>
        console.error error.stack if error?
        callback()

  getRequest: (callback) =>
    return callback() unless @_allowProcessing
    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpop @requestQueueName, @queueTimeoutSeconds, (error, result) =>
        @_updateHeartbeat()
        @_queuePool.release queueClient
        return callback error if error?
        return callback new Error 'No Result' unless result?

        [ channel, key ] = result

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
    .catch callback
    return # avoid returning pool

  start: (callback) =>
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
    @queue.empty = @enqueueJob
    @queue.drain = =>
      @_drained = true
    @enqueueJob()
    _.defer callback

  _waitForStopped: (callback) =>
    _.delay callback, 100

  _safeToStop: =>
     @_allowProcessing == false && @_drained && @_enqueuing == false

  _stopProcessing: (callback) =>
    @_allowProcessing = false
    async.doWhilst @_waitForStopped, @_safeToStop, callback

  stop: (callback) =>
    @_stopProcessing callback

module.exports = JobManagerResponder
