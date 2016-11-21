_       = require 'lodash'
async   = require 'async'
Redis   = require 'ioredis'
RedisNS = require '@octoblu/redis-ns'
UUID    = require 'uuid'
{ JobManagerRequester, JobManagerResponder } = require '..'

describe 'JobManagerRequester', ->
  beforeEach ->
    @jobTimeoutSeconds = 1
    @queueTimeoutSeconds = 1
    @jobLogSampleRate = 1
    queueId = UUID.v4()
    @requestQueueName = "request:queue:#{queueId}"
    @responseQueueName = "response:queue:#{queueId}"
    @namespace = 'test:job-manager'
    @redisUri = 'localhost'
    @maxConnections = 1

  beforeEach (done) ->
    @client = new RedisNS @namespace, new Redis @redisUri, dropBufferSupport: true
    @client.on 'ready', done

  afterEach (done) ->
    @client.del @requestQueueName, @responseQueueName, 'some-response-id', done
    return # avoid returning redis

  beforeEach (done) ->
    @sut = new JobManagerRequester {
      @namespace
      @redisUri
      @maxConnections
      @jobTimeoutSeconds
      @queueTimeoutSeconds
      @jobLogSampleRate
      @requestQueueName
      @responseQueueName
    }

    @sut.start done

  beforeEach (done) ->
    @responder = new JobManagerResponder {
      @namespace
      @redisUri
      @maxConnections
      @jobTimeoutSeconds
      @queueTimeoutSeconds
      @jobLogSampleRate
      @requestQueueName
    }

    @responder.start done

  afterEach (done) ->
    @responder.stop done

  afterEach (done) ->
    @sut.stop done

  describe '->createRequest', ->
    context 'when the maxQueueLength is exceeded', ->
      beforeEach ->
        @sut.maxQueueLength = 1

      beforeEach (done) ->
        @client.lpush @requestQueueName, 'something', done
        return # avoid returning redis

      beforeEach (done) ->
        @client.lpush @requestQueueName, 'something', done
        return # avoid returning redis

      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
            auth: uuid: 'some-uuid'

        @sut.createRequest options, (@error) => done()
        return # avoid returning redis

      it 'should return an error', ->
        expect(@error.code).to.equal 503

    context 'with override-uuids set', ->
      beforeEach ->
        @sut.jobLogSampleRateOverrideUuids = ['some-uuid']

      beforeEach (done) ->
        options =
          metadata:
            auth: uuid: 'some-uuid'
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.createRequest options, done
        @sut.emit "response:some-response-id", {}

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'request:metadata', (error, metadataStr) =>
          return done error if error?
          return done new Error 'missing metadata' unless metadataStr?
          metadata = JSON.parse metadataStr
          expect(metadata).to.containSubset
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
            jobLogs: ['override']

          done()
        return # avoid returning redis

    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.createRequest options, done

      it 'should place the job in a queue', (done) ->
        @client.brpop @requestQueueName, 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'some-response-id'
          done()
        return # avoid returning redis

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'request:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.containSubset
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
            jobLogs: ['sampled']
          done()
        return # avoid returning redis

      it 'should put the data in its place', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.be.null
          done()
        return # avoid returning redis

      it 'should put the createdAt in its place', (done) ->
        @client.hget 'some-response-id', 'request:createdAt', (error, timestamp) =>
          expect(timestamp).to.exist
          done()
        return # avoid returning redis

      it 'should expire the key', (done) ->
        @client.ttl 'some-response-id', (error, ttl) =>
          expect(ttl).to.be.at.least 0
          done()
        return # avoid returning redis

    context 'when called without a responseId', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"

        @sut.createRequest options, (error, @responseId) =>
          done error

      it 'should assign a responseId', ->
        expect(@responseId).to.exist

    context 'when called with data', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
          data:
            'tunnel-collapse': 'just a miner problem'

        @sut.createRequest options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()
        return # avoid returning redis

  describe '->createForeverRequest', ->
    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.createForeverRequest options, done

      it 'should not expire the key', (done) ->
        @client.ttl 'some-response-id', (error, ttl) =>
          expect(ttl).to.equal -1
          done()
        return # avoid returning redis

    context 'when called with data', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
          data:
            'tunnel-collapse': 'just a miner problem'

        @sut.createRequest options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()
        return # avoid returning redis

  describe '->do', ->
    context 'when called with a request', ->
      beforeEach ->
        @responder.do (request, next) =>
          { @responseId } = request.metadata

          options =
            metadata:
              gross: true
              responseId: @responseId
            rawData: 'abcd123'

          next null, options

      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.do options, (error, @response) =>
          done error

      it 'should yield the response', ->
        expect(@response).to.containSubset
          metadata:
            gross: true
            responseId: @responseId
          rawData: 'abcd123'

    context 'when called with a timed out request', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.do options, (@error) => done()

      it 'should have an error', ->
        expect(@error).to.exist
