_               = require 'lodash'
async           = require 'async'
{EventEmitter} = require 'events'

class JobManagerResponderWorker extends EventEmitter
  constructor: (options={})->
    {concurrency=1} = options
    @queue = async.queue @_work, concurrency
    @queue.empty => @emit 'empty'

  push: (request) =>
    @queue.push request

  _work: (request, callback) =>
    request.do callback

  isEmpty: =>
    return @queue.length() == 0


module.exports = JobManagerResponderWorker
