_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'
{ EventEmitter } = require 'events'

class JobManagerRequester extends EventEmitter
  constructor: (options={}) ->
    {
      @client
      @queueClient
      @jobLogSampleRate
      @timeoutSeconds
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
    throw new Error 'JobManagerRequester constructor is missing "timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "requestQueueName"' unless @requestQueueName?
    throw new Error 'JobManagerRequester constructor is missing "responseQueueName"' unless @responseQueueName?

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
    {metadata,data,rawData} = options
    metadata = _.clone metadata
    metadata.responseId ?= uuid.v4()

    @_checkMaxQueueLength (error) =>
      return callback error if error?

      metadata.jobLogs = []
      if Math.random() < @jobLogSampleRate
        metadata.jobLogs.push 'sampled'

      unless _.isEmpty @jobLogSampleRateOverrideUuids
        uuids = [ metadata.auth?.uuid, metadata.toUuid, metadata.fromUuid ]
        matches = _.intersection @jobLogSampleRateOverrideUuids, uuids
        unless _.isEmpty matches
          metadata.jobLogs.push 'override'

      if _.isEmpty metadata.jobLogs
        delete metadata.jobLogs

      if _.isArray metadata.jobLogs
        metadata.metrics = {}

      @addMetric metadata, 'enqueueRequestAt', (error) =>
        return callback error if error?
        {responseId} = metadata
        data ?= null

        metadataStr = JSON.stringify metadata
        rawData ?= JSON.stringify data

        values = [
          'request:metadata', metadataStr
          'request:data', rawData
          'request:createdAt', Date.now()
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
      @client.expire responseId, @timeoutSeconds, (error) =>
        delete error.code if error?
        callback error, responseId
    return # avoid returning redis

  do: (options, callback) =>
    options = _.clone options

    @createRequest options, (error, responseId) =>
      return callback error if error?
      @getResponse responseId, callback

  getResponse: (responseId, callback) =>
    @queueClient.brpop "#{@responseQueueName}:#{responseId}", @timeoutSeconds, (error, result) =>
      delete error.code if error?
      return callback error if error?
      unless result?
        error = new Error('Response timeout exceeded')
        error.code = 504
        return callback error, null unless result?

      [channel,key] = result

      @client.hmget key, ['response:metadata', 'response:data'], (error, data) =>
        delete error.code if error?
        return callback error if error?

        @client.del key, "#{@responseQueueName}:#{responseId}", (error) =>
          delete error.code if error?
          return callback error if error?

          [metadata, rawData] = data
          return callback new Error('Malformed response, missing metadata'), null unless metadata?

          metadata = JSON.parse metadata
          @addMetric metadata, 'dequeueResponseAt', (error) =>
            return callback error if error?

            response =
              metadata: metadata
              rawData: rawData

            callback null, response
    return # avoid returning redis

module.exports = JobManagerRequester
