_                = require 'lodash'
async            = require 'async'
debug            = require('debug')('meshblu-core-job-manager:job-manager')
UUID             = require 'uuid'
{ EventEmitter } = require 'events'

class JobManagerRequester extends EventEmitter
  constructor: (options={}) ->
    {
      @client
      @queueClient
      @jobLogSampleRate
      @jobTimeoutSeconds
      @queueTimeoutSeconds
      @maxQueueLength
      @jobLogSampleRateOverrideUuids
      @responseQueueName
      @requestQueueName
    } = options
    @maxQueueLength ?= 10000
    @jobLogSampleRateOverrideUuids ?= []

    throw new Error 'JobManagerRequester constructor is missing "client"' unless @client?
    throw new Error 'JobManagerRequester constructor is missing "queueClient"' unless @queueClient?
    throw new Error 'JobManagerRequester constructor is missing "jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'JobManagerRequester constructor is missing "jobTimeoutSeconds"' unless @jobTimeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "queueTimeoutSeconds"' unless @queueTimeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "requestQueueName"' unless @requestQueueName?
    throw new Error 'JobManagerRequester constructor is missing "responseQueueName"' unless @responseQueueName?

  _addResponseIdToOptions: (options) =>
    options = _.clone options
    { metadata } = options
    metadata = _.clone metadata
    metadata.responseId ?= @generateResponseId()
    options.metadata = metadata
    return options

  addMetric: (metadata, metricName, callback) =>
    return callback() unless _.isArray metadata.jobLogs
    metadata.metrics ?= {}
    metadata.metrics[metricName] = Date.now()
    callback()

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

  do: (options, callback) =>
    callback = _.once callback
    options = @_addResponseIdToOptions options
    responseId = _.get options, 'metadata.responseId'
    return callback new Error 'do requires metadata.responseId' unless responseId?

    @once "response:#{responseId}", (data) =>
      clearTimeout responseTimeout if responseTimeout?
      @removeListener "error:#{responseId}", callback
      callback null, data

    @once "error:#{responseId}", (error) =>
      clearTimeout responseTimeout if responseTimeout?
      @removeListener "response:#{responseId}", callback
      callback error

    @createRequest options, (error) =>
      return @emit "error:#{responseId}", error if error?
      responseTimeout = setTimeout =>
        error = new Error('Response timeout exceeded')
        error.code = 504
        @emit "error:#{responseId}", error
      , @jobTimeoutSeconds * 1000

  _emitResponses: (callback) =>
    @queueClient.brpop @responseQueueName, @queueTimeoutSeconds, (error, result) =>
      delete error.code if error?
      return callback error if error?
      return callback() unless result?

      [ channel, key ] = result

      @client.hmget key, ['response:metadata', 'response:data'], (error, data) =>
        delete error.code if error?
        return callback error if error?

        [ metadata, rawData ] = data
        return callback new Error 'Malformed response, missing metadata' if _.isEmpty metadata

        metadata = JSON.parse metadata
        { responseId } = metadata

        @addMetric metadata, 'dequeueResponseAt', (error) =>
          return callback error if error?

          response =
            metadata: metadata
            rawData: rawData

          @emit "response:#{responseId}", response
          callback()
    return # avoid returning redis

  generateResponseId: =>
    UUID.v4()

  startProcessing: =>
    @_allowProcessing = true

    async.doWhilst @_emitResponses, => @_allowProcessing

  stopProcessing: =>
    @_allowProcessing = false

module.exports = JobManagerRequester
