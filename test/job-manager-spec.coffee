_         = require 'lodash'
async     = require 'async'
redis = require 'fakeredis'
uuid      = require 'uuid'
JobManager = require '../src/job-manager'

describe 'JobManager', ->
  it 'should exist', ->
    new JobManager

  beforeEach ->
    @client = _.bindAll redis.createClient uuid.v4()

    @sut = new JobManager
      client: @client
      namespace: 'test'
      timeoutSeconds: 1

  describe '->createRequest', ->
    context 'when called with a request', ->
      beforeEach (done) ->
        options =
          responseId: 'some-response'
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response'

        @sut.createRequest options, done

      it 'should place the job in a queue', (done) ->
        @timeout 3000
        @client.brpop 'test:request:queue', 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'test:some-response'
          done()

      it 'should put the metadata in its place', (done) ->
        @client.hget 'test:some-response', 'request:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.deep.equal
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response'
          done()

      it 'should put the data in its place', (done) ->
        @client.hget 'test:some-response', 'request:data', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.be.null
          done()

  describe '->createResponse', ->
    context 'when called with a response', ->
      beforeEach (done) ->
        options =
          responseId: 'some-response'
          metadata:
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response'

        @sut.createResponse options, done

      it 'should place the job in a queue', (done) ->
        @timeout 3000
        @client.brpop 'test:response:some-response', 1, (error, result) =>
          return done(error) if error?
          [channel, responseKey] = result
          expect(responseKey).to.deep.equal 'test:some-response'
          done()

      it 'should put the metadata in its place', (done) ->
        @client.hget 'test:some-response', 'response:metadata', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.deep.equal
            duel: "i'm just in it for the glove slapping"
            responseId: 'some-response'
          done()

      it 'should put the data in its place', (done) ->
        @client.hget 'test:some-response', 'response:data', (error, metadataStr) =>
          metadata = JSON.parse metadataStr
          expect(metadata).to.be.null
          done()
