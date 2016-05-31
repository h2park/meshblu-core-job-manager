_       = require 'lodash'
async   = require 'async'
redis   = require 'fakeredis'
RedisNS = require '@octoblu/redis-ns'

uuid  = require 'uuid'
JobManager = require '../src/job-manager'

describe 'JobManager', ->
  beforeEach ->
    @redisId = uuid.v4()
    @client = new RedisNS 'ns', redis.createClient(@redisId)

    @sut = new JobManager
      client: new RedisNS 'ns', redis.createClient(@redisId)
      timeoutSeconds: 1
      jobLogSampleRate: 1

  describe 'when instantiated without a timeout', ->
    it 'should blow up', ->
      expect(=> new JobManager client: @client, jobLogSampleRate: 0).to.throw 'JobManager constructor is missing "timeoutSeconds"'

  describe 'when instantiated without a client', ->
    it 'should blow up', ->
      expect(=> new JobManager timeoutSeconds: 1, jobLogSampleRate: 0).to.throw 'JobManager constructor is missing "client"'

  describe 'when instantiated without a jobLogSampleRate', ->
    it 'should blow up', ->
      expect(=> new JobManager client: @client, timeoutSeconds: 1).to.throw 'JobManager constructor is missing "jobLogSampleRate"'

  describe 'with override-uuids set', ->
    beforeEach ->
      @client = new RedisNS 'ns', redis.createClient(@redisId)

    beforeEach ->
      @sut = new JobManager
        client: @client
        timeoutSeconds: 1
        jobLogSampleRate: 0
        overrideKey: 'override-my-uuids'

    beforeEach (done) ->
      @client.sadd 'override-my-uuids', 'some-uuid', done

    beforeEach (done) ->
      @sut.updateOverrideUuids done

    afterEach (done) ->
      @client.del 'override-my-uuids', done

    describe '->createRequest', ->
      context 'when called with a request', ->
        beforeEach (done) ->
          options =
            metadata:
              auth: uuid: 'some-uuid'
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response-id'

          @sut.createRequest 'request', options, done

        it 'should put the metadata in its place', (done) ->
          @client.hget 'some-response-id', 'request:metadata', (error, metadataStr) =>
            metadata = JSON.parse metadataStr
            expect(metadata).to.containSubset
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response-id'
              jobLogs: ['override']

            done()

  describe '->createRequest', ->
    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.createRequest 'request', options, done

      it 'should place the job in a queue', (done) ->
        @timeout 3000
        @client.brpop 'request:queue', 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'some-response-id'
          done()

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'request:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.containSubset
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
            jobLogs: ['sampled']
          done()

      it 'should put the data in its place', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.be.null
          done()

      it 'should put the createdAt in its place', (done) ->
        @client.hget 'some-response-id', 'request:createdAt', (error, timestamp) =>
          expect(timestamp).to.exist
          done()

      describe 'after the timeout has elapsed', (done) ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 0
            done()

    context 'when called without a responseId', ->
      beforeEach (done) ->
        @options =
          metadata:
            duel: "i'm just in it for the glove slapping"

        @sut.createRequest 'request', @options, (error, @responseId) =>
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

        @sut.createRequest 'request', options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()

  describe '->createForeverRequest', ->
    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @sut.createForeverRequest 'request', options, done

      describe 'after the timeout has elapsed', (done) ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 3
            done()

    context 'when called with data', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
          data:
            'tunnel-collapse': 'just a miner problem'

        @sut.createRequest 'request', options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()

  describe '->createResponse', ->
    context 'when called with a response', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
            duel: "i'm just in it for the glove slapping"

        @sut.createResponse 'response', options, done

      it 'should place the job in a queue', (done) ->
        @timeout 3000
        @client.brpop 'response:some-response-id', 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'some-response-id'
          done()

      it 'should put the metadata in its place', (done) ->
        @client.hget 'some-response-id', 'response:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.containSubset
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
          done()

      it 'should put the data in its place', (done) ->
        @client.hget 'some-response-id', 'response:data', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.be.null
          done()

      describe 'after the timeout has passed', ->
        beforeEach (done) ->
          _.delay done, 1100

        it 'should not have any data', (done) ->
          @client.hlen 'some-response-id', (error, responseKeysLength) =>
            return done error if error?
            expect(responseKeysLength).to.equal 0
            done()

    context 'when called with data', ->
      beforeEach (done) ->
        options =
          metadata:
            responseId: 'some-response-id'
          data:
            'tunnel-collapse': 'just a miner problem'

        @sut.createResponse 'request', options, done

      it 'should stringify the data', (done) ->
        @client.hget 'some-response-id', 'response:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.deep.equal 'tunnel-collapse': 'just a miner problem'
          done()

  describe '->do', ->
    context 'when called with a request', ->
      beforeEach ->
        options =
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'

        @onResponse = sinon.spy()
        @sut.do 'request', 'response', options, @onResponse

      describe 'when it receives a response', ->
        beforeEach (done) ->
          jobManager = new JobManager
            client: new RedisNS 'ns', redis.createClient(@redisId)
            timeoutSeconds: 1
            jobLogSampleRate: 0

          jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            @responseId = request.metadata.responseId

            options =
              metadata:
                gross: true
                responseId: @responseId
              rawData: 'abcd123'

            jobManager.createResponse 'response', options, done

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

  describe '->getRequest', ->
    context 'when called with a string instead of an array', ->
      beforeEach ->
        @callback = sinon.spy()
        @sut.getRequest 'hi', @callback

      it 'should blow up', ->
        [error] = @callback.firstCall.args
        expect(=> throw error).to.throw 'First argument must be an array'

    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'some-response-id'
          rawData: 'abcd123'

        @sut.createRequest 'request', options, done

      beforeEach (done) ->
        @sut.getRequest ['request'], (error, @request) =>
          done error

      it 'should return a request', ->
        expect(@request).to.exist

        expect(@request.metadata).to.containSubset
          gross: true
          responseId: 'some-response-id'

        expect(@request.rawData).to.deep.equal 'abcd123'

        expect(@request.createdAt).to.exist

    context 'when called with a two queues', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @sut.createRequest 'request2', options, done

      beforeEach (done) ->
        @sut.getRequest ['request1', 'request2'], (error, @request) =>
          done error

      it 'should return a request', ->
        expect(@request).to.exist

        expect(@request.metadata).to.containSubset
          gross: true
          responseId: 'hairball'

        expect(@request.rawData).to.deep.equal 'abcd123'

    context 'when called with a timed out request', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @sut.createRequest 'request2', options, (error) =>
          return done error if error?
          _.delay done, 1100

      beforeEach (done) ->
        @sut.getRequest ['request1', 'request2'], (error, @request) =>
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

        @sut.createResponse 'response', options, done

      beforeEach (done) ->
        @sut.getResponse 'response', 'hairball', (@error, @response) =>
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

    context 'when called with a timed out response', ->
      beforeEach (done) ->
        options =
          metadata:
            gross: true
            responseId: 'hairball'
          rawData: 'abcd123'

        @sut.createResponse 'request2', options, (error) =>
          return done error if error?
          _.delay done, 1100

      beforeEach (done) ->
        @sut.getResponse 'request2', 'hairball', (@error, @request) =>
          done()

      it 'should return an error', ->
        expect(=> throw @error).to.throw 'Response timeout exceeded'

      it 'should return a null request', ->
        expect(@request).not.to.exist

    context 'when called with and no response', ->
      beforeEach (done) ->
        @sut.getResponse 'request2', 'hairball', (@error, @request) =>
          done()

      it 'should return an error', ->
        expect(=> throw @error).to.throw 'Response timeout exceeded'

      it 'should return a null request', ->
        expect(@request).not.to.exist
