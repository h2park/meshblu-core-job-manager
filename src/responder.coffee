_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'

class JobManagerResponder
  constructor: (options={}) ->
    {
      @client
      @queueClient
      @jobLogSampleRate
      @jobTimeoutSeconds
      @queueTimeoutSeconds
      @maxQueueLength
      @jobLogSampleRateOverrideUuids
      @requestQueueName
    } = options
    @maxQueueLength ?= 10000
    @jobLogSampleRateOverrideUuids ?= []

    throw new Error 'JobManagerResponder constructor is missing "client"' unless @client?
    throw new Error 'JobManagerResponder constructor is missing "queueClient"' unless @queueClient?
    throw new Error 'JobManagerResponder constructor is missing "jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'JobManagerRequester constructor is missing "jobTimeoutSeconds"' unless @jobTimeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "queueTimeoutSeconds"' unless @queueTimeoutSeconds?
    throw new Error 'JobManagerResponder constructor is missing "requestQueueName"' unless @requestQueueName?

  addMetric: (metadata, metricName, callback) =>
    return callback() unless _.isArray metadata.jobLogs
    metadata.metrics ?= {}
    metadata.metrics[metricName] = Date.now()
    callback()

  createResponse: (options, callback) =>
    { metadata, data, rawData } = options
    { responseId } = metadata
    data ?= null
    rawData ?= JSON.stringify data

    @client.hmget responseId, ['request:metadata', 'response:queueName'], (error, result) =>
      delete error.code if error?
      return callback error if error?

      [ requestMetadata, responseQueueName ] = result ? []

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
          return callback error if error?
          return callback null, {metadata, rawData}
    return # avoid returning redis

  do: (next, callback=_.noop) =>
    async.retry @getRequest, (error, result) =>
      return callback() if error?
      return callback() if _.isEmpty result
      next result, (error, response) =>
        return callback error if error?
        @createResponse response, callback

  getRequest: (callback) =>
    @queueClient.brpop @requestQueueName, @queueTimeoutSeconds, (error, result) =>
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
    return # avoid returning redis

module.exports = JobManagerResponder
