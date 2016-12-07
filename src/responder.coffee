_              = require 'lodash'
async          = require 'async'
debug          = require('debug')('meshblu-core-job-manager:job-manager')
JobManagerBase = require './base'

class JobManagerResponder extends JobManagerBase
  constructor: (options={}) ->
    {
      @jobTimeoutSeconds
      @queueTimeoutSeconds
      @requestQueueName
    } = options

    throw new Error 'JobManagerRequester constructor is missing "jobTimeoutSeconds"' unless @jobTimeoutSeconds?
    throw new Error 'JobManagerRequester constructor is missing "queueTimeoutSeconds"' unless @queueTimeoutSeconds?
    throw new Error 'JobManagerResponder constructor is missing "requestQueueName"' unless @requestQueueName?

    super

  createResponse: (options, callback) =>
    { metadata, data, rawData } =options
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
          callback null, { metadata, rawData }

    return # avoid returning redis

  do: (next, callback=_.noop) =>
    async.retry @getRequest, (error, result) =>
      return callback() if error?
      return callback() if _.isEmpty result
      next result, (error, response) =>
        return callback error if error?
        @createResponse response, callback

  getRequest: (callback) =>
    @_queuePool.acquire().then (queueClient) =>
      queueClient.brpop @requestQueueName, @queueTimeoutSeconds, (error, result) =>
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
      callback()
    .catch callback
    return # nothing

  stop: (callback) =>
    @_commandPool.release @client
    .then =>
      return @_commandPool.drain()
    .then =>
      return @_queuePool.drain()
    .then =>
      callback()
    .catch callback
    return # nothing

module.exports = JobManagerResponder
