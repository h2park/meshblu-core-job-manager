_       = require 'lodash'
async   = require 'async'
redis   = require 'fakeredis'
RedisNS = require '@octoblu/redis-ns'

uuid  = require 'uuid'
JobManager = require '../src/job-manager'

describe 'JobManager', ->
  it 'should exist', ->
    new JobManager

  beforeEach ->
    @client = new RedisNS 'ns', redis.createClient()

    @sut = new JobManager
      client: @client
      timeoutSeconds: 1

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
          expect(metadata).to.deep.equal
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response-id'
          done()

      it 'should put the data in its place', (done) ->
        @client.hget 'some-response-id', 'request:data', (error, dataStr) =>
          data = JSON.parse dataStr
          expect(data).to.be.null
          done()

      describe 'after the timeout has elapsed', (done) ->
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
          expect(metadata).to.deep.equal
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

  describe '->getRequest', ->
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

        expect(@request.metadata).to.deep.equal
          gross: true
          responseId: 'some-response-id'

        expect(@request.rawData).to.deep.equal 'abcd123'

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

        expect(@request.metadata).to.deep.equal
          gross: true
          responseId: 'hairball'

        expect(@request.rawData).to.deep.equal 'abcd123'

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

        expect(@response.metadata).to.deep.equal
          gross: true
          responseId: 'hairball'

        expect(@response.rawData).to.deep.equal 'abcd123'
