_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')
uuid  = require 'uuid'

class JobManager
  constructor: (options={}) ->
    {@client,@timeoutSeconds} = options
    throw new Error 'JobManager constructor is missing "timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'JobManager constructor is missing "client"' unless @client?

  createRequest: (requestQueue, options, callback) =>
    {metadata,data,rawData} = options
    {responseId} = metadata
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{responseId}", 'request:metadata', metadataStr
    debug '@client.lpush', "#{requestQueue}:queue"

    async.series [
      async.apply @client.hset, "#{responseId}", 'request:metadata', metadataStr
      async.apply @client.hset, "#{responseId}", 'request:data', rawData
      async.apply @client.expire, "#{responseId}", @timeoutSeconds
      async.apply @client.lpush, "#{requestQueue}:queue", "#{responseId}"
    ], (error) =>
      delete error.code if error?
      callback error

  createResponse: (responseQueue, options, callback) =>
    {metadata,data,rawData} = options
    {responseId} = metadata
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{responseId}", 'response:metadata', metadataStr
    debug "@client.expire", "#{responseId}", @timeoutSeconds
    debug "@client.lpush", "#{responseQueue}:#{responseId}", "#{responseId}"
    async.series [
      async.apply @client.hset, "#{responseId}", 'response:metadata', metadataStr
      async.apply @client.hset, "#{responseId}", 'response:data', rawData
      async.apply @client.expire, "#{responseId}", @timeoutSeconds
      async.apply @client.lpush, "#{responseQueue}:#{responseId}", "#{responseId}"
      async.apply @client.expire, "#{responseQueue}:#{responseId}", @timeoutSeconds
    ], (error) =>
      delete error.code if error?
      callback error

  do: (requestQueue, responseQueue, options, callback) =>
    options = _.clone options
    options.metadata.responseId ?= uuid.v4()
    {responseId} = options.metadata

    @createRequest requestQueue, options, =>
      @getResponse responseQueue, responseId, callback

  getRequest: (requestQueues, callback) =>
    return callback new Error 'First argument must be an array' unless _.isArray requestQueues
    queues = _.map requestQueues, (queue) => "#{queue}:queue"
    debug '@client.brpop', queues...
    @client.brpop queues..., @timeoutSeconds, (error, result) =>
      return callback error if error?
      return callback null, null unless result?

      [channel,key] = result

      async.parallel
        metadata: async.apply @client.hget, key, 'request:metadata'
        data: async.apply @client.hget, key, 'request:data'
      , (error, result) =>
        delete error.code if error?
        return callback error if error?
        return callback null, null unless result.metadata?

        callback null,
          metadata: JSON.parse result.metadata
          rawData: result.data

  getResponse: (responseQueue, responseId, callback) =>
    debug '@client.brpop', "#{responseQueue}:#{responseId}"
    @client.brpop "#{responseQueue}:#{responseId}", @timeoutSeconds, (error, result) =>
      delete error.code if error?
      return callback error if error?
      return callback new Error('Response timeout exceeded'), null unless result?

      [channel,key] = result

      async.parallel
        metadata: async.apply @client.hget, key, 'response:metadata'
        data: async.apply @client.hget, key, 'response:data'
      , (error, result) =>
        delete error.code if error?
        return callback error if error?
        return callback new Error('Response timeout exceeded'), null unless result.metadata?

        callback null,
          metadata: JSON.parse result.metadata
          rawData: result.data

module.exports = JobManager
