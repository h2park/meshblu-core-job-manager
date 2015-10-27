async = require 'async'
debug = require('debug')('meshblu-http-server:redis-job')

class JobManager
  constructor: (options={}) ->
    {@namespace,@client,@timeoutSeconds} = options
    {@requestQueue,@responseQueue} = options
    @requestQueue ?= 'request'
    @responseQueue ?= 'response'

  getResponse: (key, callback) =>
    @client.brpop key, @timeoutSeconds, (error, result) =>
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

  createRequest: (options, callback) =>
    {metadata,data,responseId,rawData} = options
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{@namespace}:#{responseId}", 'request:metadata', metadataStr
    async.series [
      async.apply @client.hset, "#{@namespace}:#{responseId}", 'request:metadata', metadataStr
      async.apply @client.hset, "#{@namespace}:#{responseId}", 'request:data', rawData
      async.apply @client.lpush, "#{@namespace}:#{@requestQueue}:queue", "#{@namespace}:#{responseId}"
    ], callback

  createResponse: (options, callback)=>
    {metadata,data,responseId,rawData} = options
    data ?= null

    metadataStr = JSON.stringify metadata
    rawData ?= JSON.stringify data

    debug "@client.hset", "#{@namespace}:#{responseId}", 'response:metadata', metadataStr
    async.series [
      async.apply @client.hset, "#{@namespace}:#{responseId}", 'response:metadata', metadataStr
      async.apply @client.hset, "#{@namespace}:#{responseId}", 'response:data', rawData
      async.apply @client.lpush, "#{@namespace}:#{@responseQueue}:#{responseId}", "#{@namespace}:#{responseId}"
    ], callback

module.exports = JobManager
