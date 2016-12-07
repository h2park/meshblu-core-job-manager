_                = require 'lodash'
async            = require 'async'
debug            = require('debug')('meshblu-core-job-manager:job-manager')
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

      unless _.isEmpty @jobLogSampleRateOverrideUuids
        uuids = [ metadata.auth?.uuid, metadata.toUuid, metadata.fromUuid, metadata.auth?.as ]
        matches = _.intersection @jobLogSampleRateOverrideUuids, uuids
        unless _.isEmpty matches
          metadata.jobLogs.push 'override'

      if _.isEmpty metadata.jobLogs
        delete metadata.jobLogs

      if _.isArray metadata.jobLogs
        metadata.metrics = {}

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

  _emitResponses: (callback) =>
    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpop @responseQueueName, @queueTimeoutSeconds, (error, result) =>
        @_updateHeartbeat()
        @_queuePool.release queueClient
        console.error error.stack if error? # log error and continue
        return callback() if _.isEmpty result
        return callback() unless @_allowProcessing

        [ channel, key ] = result
        @_getResponse key, (error, response) =>
          console.error error.stack if error? # log error and continue
          return callback() if _.isEmpty response
          responseId = _.get response, 'metadata.responseId'

          @emit "response:#{responseId}", response
          callback()
    .catch callback
    return # nothing

  _getResponse: (key, callback) =>
    @client.hmget key, ['response:metadata', 'response:data'], (error, data) =>
      delete error.code if error?
      return callback error if error?

      [ metadata, rawData ] = data
      return callback new Error 'Malformed response, missing metadata' if _.isEmpty metadata

      metadata = JSON.parse metadata

      @addMetric metadata, 'dequeueResponseAt', (error) =>
        return callback error if error?

        response =
          metadata: metadata
          rawData: rawData

        callback null, response
    return # avoid returning redis

  generateResponseId: =>
    UUID.v4()

  start: (callback) =>
    @_commandPool.acquire().then (@client) =>
      @client.once 'error', (error) =>
        @emit 'error', error

      @_startProcessing callback

    .catch callback
    return # nothing

  _startProcessing: (callback) =>
    @_allowProcessing = true
    @_stoppedProcessing = false
    async.doWhilst @_emitResponses, (=> @_allowProcessing), (error) =>
      @emit 'error', error if error?
      @_stoppedProcessing = true
    _.defer callback

  _stopProcessing: (callback) =>
    @_allowProcessing = false
    async.doWhilst @_waitForStopped, (=> @_stoppedProcessing), callback

  _waitForStopped: (callback) =>
    _.delay callback, 100

  stop: (callback) =>
    @_stopProcessing (error) =>
      @_commandPool.release @client
        .then =>
          return @_commandPool.drain()
        .then =>
          return @_queuePool.drain()
        .then =>
          callback error
    return # nothing

module.exports = JobManagerRequester
