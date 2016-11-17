_                   = require 'lodash'
async               = require 'async'
Redis               = require 'ioredis'
RedisNS             = require '@octoblu/redis-ns'
uuid                = require 'uuid'
JobManagerRequester = require '../src/requester'
JobManagerResponder = require '../src/responder'

describe 'JobManagerRequester', ->
  beforeEach ->
    @timeoutSeconds = 1
    @jobLogSampleRate = 1
    @requestQueueName = 'request:queue'
    @responseQueueName = 'response:queue'

  beforeEach (done) ->
    @client = new RedisNS 'test-job-manager', new Redis 'localhost', dropBufferSupport: true
    @client.on 'ready', done

  beforeEach (done) ->
    @queueClient = new RedisNS 'test-job-manager', new Redis 'localhost', dropBufferSupport: true
    @queueClient.on 'ready', done

  afterEach (done) ->
    @client.del @requestQueueName, @responseQueueName, 'some-response-id', done
    return # avoid returning redis

  beforeEach ->
    @sut = new JobManagerRequester {
      @client
      @queueClient
      @timeoutSeconds
      @jobLogSampleRate
      @requestQueueName
      @responseQueueName
    }

    @responder = new JobManagerResponder {
      @client
      @queueClient
      @timeoutSeconds
      @jobLogSampleRate
      @requestQueueName
      @responseQueueName
    }

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

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'request:metadata', (error, metadataStr) =>
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
        @timeout 3000
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

      describe 'after the timeout has elapsed', (done) ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 0
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

      describe 'after the timeout has elapsed', (done) ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 3
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

  xdescribe '->createResponse', ->
    context 'when called with a ignoreResponse', ->
      beforeEach (done) ->
        data =
          ignoreResponse: true
        @client.hset 'ignored-response-id', 'request:metadata', JSON.stringify(data), done
        return # avoid returning redis

      beforeEach (done) ->
        options =
          metadata:
            responseId: 'ignored-response-id'
            duel: "i'm just in it for the glove slapping"

        @sut.createResponse options, done

      it 'should not place the job in a queue', (done) ->
        @client.llen 'response:ignored-response-id', (error, result) =>
          return done(error) if error?
          expect(result).to.equal 0
          done()
        return # avoid returning redis

    context 'when called with a response', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
            duel: "i'm just in it for the glove slapping"

        @sut.createResponse options, done

      it 'should place the job in a queue', (done) ->
        @timeout 3000
        @client.brpop 'response:some-response-id', 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'some-response-id'
          done()
        return # avoid returning redis

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'response:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.containSubset
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
          done()
        return # avoid returning redis

      it 'should put the data in its place', (done) ->
        @client.hget 'some-response-id', 'response:data', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.be.null
          done()
        return # avoid returning redis

      describe 'after the timeout has passed', ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 0
            done()
          return # avoid returning redis

    context 'when called with data', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
          data:
            'tunnel-collapse': 'just a miner problem'

        @sut.createResponse options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'response:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()
        return # avoid returning redis

  describe '->do', ->
    context 'when called with a request', ->
      beforeEach ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @onResponse = sinon.spy()
        @sut.do options, @onResponse

      describe 'when it receives a response', ->
        beforeEach (done) ->
          @responder.getRequest (error, request) =>
            return done error if error?
            @responseId = request.metadata.responseId

            options =
              metadata:
                gross: true
                responseId: @responseId
              rawData: 'abcd123'

            @responder.createResponse options, done

        it 'should yield the response', (done) ->
          onResponseCalled = => @onResponse.called
          wait = (callback) => _.delay callback, 10

          async.until onResponseCalled, wait, =>
            expect(@onResponse.args[0][1]).to.containSubset
              metadata:
                gross: true
                responseId: @responseId
              rawData: 'abcd123'
            done()
          return # promises

    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'some-response-id'
          rawData: 'abcd123'

        @sut.createRequest options, done

      beforeEach (done) ->
        @responder.getRequest (error, @request) =>
          done error

      it 'should return a request', ->
        expect(@request).to.exist

        expect(@request.metadata).to.containSubset
          gross: true
          responseId: 'some-response-id'

        expect(@request.rawData).to.deep.equal 'abcd123'

        expect(@request.createdAt).to.exist

    context 'when called with a timed out request', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @sut.createRequest options, (error) =>
          return done error if error?
          _.delay done, 1100

      beforeEach (done) ->
        @responder.getRequest (error, @request) =>
          done error

      it 'should return a null request', ->
        expect(@request).not.to.exist

  describe '->getResponse', ->
    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @responder.createResponse options, done

      beforeEach (done) ->
        @sut.getResponse 'hairball', (@error, @response) =>
          done()

      it 'should return a response', ->
        expect(@response).to.exist

        expect(@response.metadata).to.containSubset
          gross: true
          responseId: 'hairball'

        expect(@response.rawData).to.deep.equal 'abcd123'

      it 'should clean up', (done) ->
        @client.exists 'hairball', (error, exists) =>
          return done error if error?
          expect(exists).to.equal 0
          done()
        return # avoid returning redis

    context 'when called with a timed out response', ->
      @timeout 3000
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @responder.createResponse options, (error) =>
          return done error if error?
          _.delay done, 1100

      beforeEach (done) ->
        @sut.getResponse 'hairball', (@error, @request) =>
          done()

      it 'should return an error', ->
        expect(=> throw @error).to.throw 'Response timeout exceeded'

      it 'should return a null request', ->
        expect(@request).not.to.exist

    context 'when called with and no response', ->
      beforeEach (done) ->
        @sut.getResponse 'hairball', (@error, @request) =>
          done()

      it 'should return an error', ->
        expect(=> throw @error).to.throw 'Response timeout exceeded'

      it 'should return a null request', ->
        expect(@request).not.to.exist
