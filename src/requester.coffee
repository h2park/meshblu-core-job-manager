_                = require 'lodash'
async            = require 'async'
UUID             = require 'uuid'
JobManagerBase   = require './base'

class JobManagerRequester extends JobManagerBase
  constructor: (options={}) ->
    {
      @jobLogSampleRate
      @maxQueueLength
      @jobLogSampleRateOverrideUuids
      @responseQueueName
      @requestQueueName
    } = options
    @maxQueueLength ?= 10000
    @jobLogSampleRateOverrideUuids ?= []

    throw new Error 'JobManagerRequester constructor is missing "jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'JobManagerRequester constructor is missing "requestQueueName"' unless @requestQueueName?
    throw new Error 'JobManagerRequester constructor is missing "responseQueueName"' unless @responseQueueName?

    super

  _addResponseIdToOptions: (options) =>
    options = _.clone options
    { metadata } = options
    metadata = _.clone metadata
    metadata.responseId ?= @generateResponseId()
    options.metadata = metadata
    return options

  _checkMaxQueueLength: (callback) =>
    return callback() unless @maxQueueLength > 0
    @client.llen @requestQueueName, (error, queueLength) =>
      return callback error if error?
      return callback() if queueLength <= @maxQueueLength

      error = new Error 'Maximum Capacity Exceeded'
      error.code = 503
      callback error
    return # avoid returning redis

  createForeverRequest: (options, callback) =>
    options = @_addResponseIdToOptions options
    {metadata,data,rawData} = options

    @_checkMaxQueueLength (error) =>
      return callback error if error?

      metadata.jobLogs = []
      if Math.random() < @jobLogSampleRate
        metadata.jobLogs.push 'sampled'

      uuids = [ metadata.auth?.uuid, metadata.toUuid, metadata.fromUuid, metadata.auth?.as ]
      metadata.jobLogs.push 'override' unless _.isEmpty _.intersection @jobLogSampleRateOverrideUuids, uuids

      @addMetric metadata, 'enqueueRequestAt', (error) =>
        return callback error if error?
        { responseId } = metadata
        data ?= null

        metadataStr = JSON.stringify metadata
        rawData ?= JSON.stringify data

        values = [
          'request:metadata', metadataStr
          'request:data', rawData
          'request:createdAt', Date.now()
          'response:queueName', @responseQueueName
        ]

        async.series [
          async.apply @client.hmset, responseId, values
          async.apply @client.lpush, @requestQueueName, responseId
        ], (error) =>
          delete error.code if error?
          callback error, responseId
    return # avoid returning redis

  createRequest: (options, callback) =>
    @createForeverRequest options, (error, responseId) =>
      return callback error if error?
      @client.expire responseId, @jobTimeoutSeconds, (error) =>
        delete error.code if error?
        return callback error if error?
        callback null, responseId

    return # avoid returning redis

  do: (request, callback) =>
    callback = _.once callback
    request = @_addResponseIdToOptions request
    responseId = _.get request, 'metadata.responseId'
    responseTimeout = null
    return callback new Error 'do requires metadata.responseId' unless responseId?

    @once "response:#{responseId}", (response) =>
      clearTimeout responseTimeout if responseTimeout?
      @removeListener "error:#{responseId}", callback
      callback null, response

    @once "error:#{responseId}", (error) =>
      clearTimeout responseTimeout if responseTimeout?
      @removeListener "response:#{responseId}", callback
      callback error

    @createRequest request, (error) =>
      return @emit "error:#{responseId}", error if error?
      responseTimeout = setTimeout =>
        error = new Error('Response timeout exceeded')
        error.code = 504
        @emit "error:#{responseId}", error
      , @jobTimeoutSeconds * 1000

  _listenForResponses: (callback) =>
    @pubSubClient.subscribe @responseQueueName
    @pubSubClient.on 'message', (channel, data) =>
      @_updateHeartbeat()
      try
        data = JSON.parse data
      catch error

      { metadata, rawData } = data

      @_parseResponse { metadata, rawData }, (error, response) =>
        console.error error.stack if error? # log error and continue
        return if _.isEmpty response
        responseId = _.get response, 'metadata.responseId'

        @emit "response:#{responseId}", response

  _emitResponses: (callback) =>
    return _.defer callback unless @_allowProcessing
    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpop @responseQueueName, @queueTimeoutSeconds, (error, result) =>
        @_updateHeartbeat()
        @_queuePool.release queueClient
        console.error error.stack if error? # log error and continue
        return callback() if _.isEmpty result
        return callback() # callback early to allow brpop to keep going

        [ channel, key ] = result
        @_getResponse key, (error, response) =>
          console.error error.stack if error? # log error and continue
          return if _.isEmpty response
          responseId = _.get response, 'metadata.responseId'

          @emit "response:#{responseId}", response
    .catch callback
    return # nothing

  _getResponse: (key, callback) =>
    @client.hmget key, ['response:metadata', 'response:data'], (error, data) =>
      delete error.code if error?
      return callback error if error?
      [ metadata, rawData ] = data
      metadata = JSON.parse metadata
      @_parseResponse { metadata, rawData }, callback
    return # promises

  _parseResponse: ({ metadata, rawData }, callback) =>
    return if _.isEmpty metadata

    @addMetric metadata, 'dequeueResponseAt', (error) =>
      return callback error if error?

      response =
        metadata: metadata
        rawData: rawData

      callback null, response

  generateResponseId: =>
    UUID.v4()

  start: (callback=_.noop) =>
    @_commandPool.acquire()
    .then (@client) =>
      @client.once 'error', (error) =>
        @emit 'error', error

      @_pubSubPool.acquire()
    .then (@pubSubClient) =>
      @_startProcessing callback
    .catch callback
    return # nothing

  _startProcessing: (callback) =>
    callback = _.once callback
    @_allowProcessing = true

    @_listenForResponses()
    _.defer callback

  _stopProcessing: (callback) =>
    @_allowProcessing = false
    @pubSubClient.unsubscribe @responseQueueName
    callback()

  stop: (callback=_.noop) =>
    @_stopProcessing (error) =>
      @_pubSubPool.release @pubSubClient
      .then =>
        @_commandPool.release @client
      .then =>
        return @_pubSubPool.drain()
      .then =>
        return @_commandPool.drain()
      .then =>
        return @_queuePool.drain()
      .then =>
        callback error
      .catch callback
    return # nothing

module.exports = JobManagerRequester
