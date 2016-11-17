_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'
{ EventEmitter } = require 'events'

class JobManagerResponder extends EventEmitter
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

    throw new Error 'JobManagerResponder constructor is missing "client"' unless @client?
    throw new Error 'JobManagerResponder constructor is missing "queueClient"' unless @queueClient?
    throw new Error 'JobManagerResponder constructor is missing "jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'JobManagerResponder constructor is missing "timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'JobManagerResponder constructor is missing "requestQueueName"' unless @requestQueueName?
    throw new Error 'JobManagerResponder constructor is missing "responseQueueName"' unless @responseQueueName?

  addMetric: (metadata, metricName, callback) =>
    return callback() unless _.isArray metadata.jobLogs
    metadata.metrics ?= {}
    metadata.metrics[metricName] = Date.now()
    callback()

  createResponse: (options, callback) =>
    {metadata,data,rawData} = options
    {responseId} = metadata
    data ?= null
    rawData ?= JSON.stringify data

    @client.hget responseId, 'request:metadata', (error, result) =>
      delete error.code if error?
      return callback error if error?

      try
        requestMetadata = JSON.parse result
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
          async.apply @client.expire, responseId, @timeoutSeconds
          async.apply @client.lpush, "#{@responseQueueName}:#{responseId}", responseId
          async.apply @client.expire, "#{@responseQueueName}:#{responseId}", @timeoutSeconds
        ], (error) =>
          delete error.code if error?
          return callback error if error?
          return callback null, {metadata, rawData}
    return # avoid returning redis

  getRequest: (callback) =>
    @queueClient.brpop @requestQueueName, @timeoutSeconds, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [channel,key] = result

      @client.hgetall key, (error, result) =>
        delete error.code if error?
        return callback error if error?
        return callback() unless result?
        return callback() unless result['request:metadata']?

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
