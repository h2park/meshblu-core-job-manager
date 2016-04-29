_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'

class JobManager
  constructor: (options={}) ->
    {@client,@timeoutSeconds} = options
    throw new Error 'JobManager constructor is missing "timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'JobManager constructor is missing "client"' unless @client?

  createForeverRequest: (requestQueue, options, callback) =>
    {metadata,data,rawData,ignoreResponse} = options
    metadata.responseId ?= uuid.v4()
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
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    values = [
      'response:metadata', metadataStr
      'response:data', rawData
    ]

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
      return callback null, null unless result?

      [channel,key] = result

      @client.hgetall key, (error, result) =>
        delete error.code if error?
        return callback error if error?

        _.each result, (value, key) =>
          newKey = _.last _.split key, /:/
          result[newKey] = value
          delete result[key]

        if result.ignoreResponse?
          @client.del key, ->
        return callback null, null unless result.metadata?

        callback null,
          createdAt: result.createdAt
          metadata: JSON.parse result.metadata
          rawData: result.data

  getResponse: (responseQueue, responseId, callback) =>
    @client.brpop "#{responseQueue}:#{responseId}", @timeoutSeconds, (error, result) =>
      delete error.code if error?
      return callback error if error?
      return callback new Error('Response timeout exceeded'), null unless result?

      [channel,key] = result

      async.parallel
        metadata: async.apply @client.hget, key, 'response:metadata'
        data: async.apply @client.hget, key, 'response:data'
        del: async.apply @client.del, key, "#{responseQueue}:#{responseId}" # clean up
      , (error, result) =>
        delete error.code if error?
        return callback error if error?
        return callback new Error('Response timeout exceeded'), null unless result.metadata?

        callback null,
          metadata: JSON.parse result.metadata
          rawData: result.data

module.exports = JobManager
