_     = require 'lodash'
async = require 'async'
debug = require('debug')('meshblu-core-job-manager:job-manager')

class JobManager
  constructor: (options={}) ->
    {client,@timeoutSeconds} = options
    @client = _.bindAll client
    {@requestQueue,@responseQueue} = options
    @requestQueue ?= 'request'
    @responseQueue ?= 'response'

  getResponse: (responseId, callback) =>
    debug '@client.brpop', "#{@responseQueue}:#{responseId}"
    @client.brpop "#{@responseQueue}:#{responseId}", @timeoutSeconds, (error, result) =>
      return callback error if error?

      return callback null, null unless result?

      [channel,key] = result

      async.parallel
        metadata: async.apply @client.hget, key, 'response:metadata'
        data: async.apply @client.hget, key, 'response:data'
      , (error, result) =>
        return callback error if error?

        callback null,
          metadata: JSON.parse result.metadata
          rawData: result.data

  getRequest: (callback) =>
    debug '@client.brpop', "#{@requestQueue}:queue"
    @client.brpop "#{@requestQueue}:queue", @timeoutSeconds, (error, result) =>
      return callback error if error?
      return callback null, null unless result?

      [channel,key] = result

      async.parallel
        metadata: async.apply @client.hget, key, 'request:metadata'
        data: async.apply @client.hget, key, 'request:data'
      , (error, result) =>
        return callback error if error?

        callback null,
          metadata: JSON.parse result.metadata
          rawData: result.data

  createRequest: (options, callback) =>
    {metadata,data,responseId,rawData} = options
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{responseId}", 'request:metadata', metadataStr
    debug '@client.lpush', "#{@requestQueue}:queue"
    async.series [
      async.apply @client.hset, "#{responseId}", 'request:metadata', metadataStr
      async.apply @client.hset, "#{responseId}", 'request:data', rawData
      async.apply @client.lpush, "#{@requestQueue}:queue", "#{responseId}"
    ], callback

  createResponse: (options, callback)=>
    {metadata,data,responseId,rawData} = options
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{responseId}", 'response:metadata', metadataStr
    debug "@client.lpush", "#{@responseQueue}#{responseId}", "#{responseId}"
    async.series [
      async.apply @client.hset, "#{responseId}", 'response:metadata', metadataStr
      async.apply @client.hset, "#{responseId}", 'response:data', rawData
      async.apply @client.lpush, "#{@responseQueue}:#{responseId}", "#{responseId}"
    ], callback

module.exports = JobManager
