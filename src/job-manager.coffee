_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'

class JobManager
  constructor: (options={}) ->
    {@client,@timeoutSeconds} = options
    throw new Error 'JobManager constructor is missing "timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'JobManager constructor is missing "client"' unless @client?

  addMetric: (metadata, metricName, overwrite) =>
    metadata.metrics ?= {}
    if overwrite
      metadata.metrics[metricName] = Date.now()
    else
      metadata.metrics[metricName] ?= Date.now()

  createForeverRequest: (requestQueue, options, callback) =>
    {metadata,data,rawData,ignoreResponse} = options
    metadata.responseId ?= uuid.v4()
    @addMetric metadata, 'enqueueRequestAt'
    {responseId} = metadata
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    values = [
      'request:metadata', metadataStr
      'request:data', rawData
      'request:createdAt', Date.now()
    ]

    if ignoreResponse
      values.push 'request:ignoreResponse'
      values.push 1

    async.series [
      async.apply @client.hmset, responseId, values
      async.apply @client.lpush, "#{requestQueue}:queue", responseId
    ], (error) =>
      delete error.code if error?
      callback error

  createRequest: (requestQueue, options, callback) =>
    @createForeverRequest requestQueue, options, (error) =>
      return callback error if error?
      {responseId} = options.metadata
      @client.expire responseId, @timeoutSeconds, (error) =>
        delete error.code if error?
        callback error

  createResponse: (responseQueue, options, callback) =>
    {metadata,data,rawData} = options
    {responseId} = metadata
    @addMetric metadata, 'enqueueResponseAt', true
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    values = [
      'response:metadata', metadataStr
      'response:data', rawData
    ]

    @client.hexists responseId, 'request:ignoreResponse', (error, ignoreResponse) =>
      delete error.code if error?
      return callback error if error?

      if ignoreResponse == 1
        @client.del responseId, (error) =>
          delete error.code if error?
          callback error
        return

      async.series [
        async.apply @client.hmset, responseId, values
        async.apply @client.expire, responseId, @timeoutSeconds
        async.apply @client.lpush, "#{responseQueue}:#{responseId}", responseId
        async.apply @client.expire, "#{responseQueue}:#{responseId}", @timeoutSeconds
      ], (error) =>
        delete error.code if error?
        callback error

  do: (requestQueue, responseQueue, options, callback) =>
    options = _.clone options

    @createRequest requestQueue, options, =>
      {responseId} = options.metadata
      @getResponse responseQueue, responseId, callback

  getRequest: (requestQueues, callback) =>
    return callback new Error 'First argument must be an array' unless _.isArray requestQueues
    queues = _.map requestQueues, (queue) => "#{queue}:queue"
    @client.brpop queues..., @timeoutSeconds, (error, result) =>
      return callback error if error?
      return callback() unless result?
      dequeueRequestAt = Date.now()

      [channel,key] = result

      @client.hgetall key, (error, result) =>
        delete error.code if error?
        return callback error if error?
        return callback() unless result?
        return callback() unless result['request:metadata']?

        metadata = JSON.parse result['request:metadata']
        @addMetric metadata, 'dequeueRequestAt'

        request =
          createdAt: result['request:createdAt']
          metadata:  metadata
          rawData:   result['request:data']

        callback null, request

  getResponse: (responseQueue, responseId, callback) =>
    @client.brpop "#{responseQueue}:#{responseId}", @timeoutSeconds, (error, result) =>
      delete error.code if error?
      return callback error if error?
      return callback new Error('Response timeout exceeded'), null unless result?
      dequeueResponseAt = Date.now()

      [channel,key] = result

      @client.hmget key, ['response:metadata', 'response:data'], (error, data) =>
        delete error.code if error?
        return callback error if error?

        @client.del key, "#{responseQueue}:#{responseId}", (error) =>
          delete error.code if error?
          return callback error if error?

          [metadata, rawData] = data
          return callback new Error('Response timeout exceeded'), null unless metadata?

          metadata = JSON.parse metadata
          @addMetric metadata, 'dequeueResponseAt', true

          response =
            metadata: metadata
            rawData: rawData

          callback null, response

module.exports = JobManager
