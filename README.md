# meshblu-core-job-manager
Meshblu Core Job Manager

[![Build Status](https://travis-ci.org/octoblu/meshblu-core-job-manager.svg?branch=master)](https://travis-ci.org/octoblu/meshblu-core-job-manager)
[![Test Coverage](https://codecov.io/gh/octoblu/meshblu-core-job-manager/branch/master/graph/badge.svg)](https://codecov.io/gh/octoblu/meshblu-core-job-manager)
[![Dependency status](http://img.shields.io/david/octoblu/meshblu-core-job-manager.svg?style=flat)](https://david-dm.org/octoblu/meshblu-core-job-manager)
[![devDependency Status](http://img.shields.io/david/dev/octoblu/meshblu-core-job-manager.svg?style=flat)](https://david-dm.org/octoblu/meshblu-core-job-manager#info=devDependencies)
[![Slack Status](http://community-slack.octoblu.com/badge.svg)](http://community-slack.octoblu.com)

[![NPM](https://nodei.co/npm/meshblu-core-job-manager.svg?style=flat)](https://npmjs.org/package/meshblu-core-job-manager)



# Force logging of particular UUIDs

Add the uuid to the redis set specified in `overrideKey`. (defaults to `override-uuids`)

```
redis-cli SADD meshblu:override-uuids b3ad254f-0ef1-401b-9c1b-22c429a60208
```

# Change maxQueueLength

Set 'request:max-queue-length' to an integer value. If unset, queue length is unlimited

```
redis-cli SET meshblu:request:max-queue-length 1000
```
