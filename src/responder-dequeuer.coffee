_              = require 'lodash'
async          = require 'async'
JobManagerBase = require './base'
debug          = require('debug')('meshblu-core-job-manager:responder')
debugSaturation = require('debug')('meshblu-core-job-manager:responder:saturation')
SimpleBenchmark = require 'simple-benchmark'

class ResponderDequeuer
  constructor: ({ @queue, @_queuePool, @_updateHeartbeat, @requestQueueName, @queueTimeoutSeconds, @x, @onPush }) ->
    @onPush ?= _.noop
    @_updateHeartbeat ?= _.noop

  start: (callback=_.noop) =>
    @_allowProcessing = true
    @_getNewQueueClient().then =>
      async.doWhilst @enqueueJob, (=> @_allowProcessing)
      callback()
    .catch callback

  stop: (callback=_.noop) =>
    @_allowProcessing = false
    async.doUntil @_waitForStopped, @_safeToStop, =>
      @_queuePool.release @queueClient
      .catch (error) =>
        console.error error
      callback()
    return # promises

  _waitForStopped: (callback) =>
    _.delay callback, 100

  _safeToStop: =>
    @_allowProcessing == false && @_enqueuing == false

  _getNewQueueClient: (error) =>
    console.error error.stack if error?
    @_queuePool.acquire().then (@queueClient) =>
      @queueClient.once 'error', @_getNewQueueClient

  enqueueJob: (callback=_.noop) =>
    return _.defer callback if @queue.length() > @queue.concurrency

    benchmark = new SimpleBenchmark label: "enqueueJob #{@x}"
    @_enqueuing = true
    @dequeueJob (error, key) =>
      debug benchmark.toString()
      # order is important here
      @_enqueuing = false
      return callback() if error?
      return callback() if _.isEmpty key
      @onPush()
      @queue.push key
      debugSaturation @x, "ql:", @queue.length(), "wl:", @queue.workersList().length
      callback()

  dequeueJob: (callback) =>
    @queueClient.brpop @requestQueueName, @queueTimeoutSeconds, (error, result) =>
      @_updateHeartbeat()
      return callback error if error?
      return callback new Error 'No Result' unless result?

      [ channel, key ] = result
      return callback null, key

module.exports = ResponderDequeuer
