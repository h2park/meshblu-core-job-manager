# meshblu-core-job-manager
Meshblu Core Job Manager

[![Build Status](https://travis-ci.org/octoblu/meshblu-core-job-manager.svg?branch=master)](https://travis-ci.org/octoblu/meshblu-core-job-manager)
[![Code Climate](https://codeclimate.com/github/octoblu/meshblu-core-job-manager/badges/gpa.svg)](https://codeclimate.com/github/octoblu/meshblu-core-job-manager)
[![Test Coverage](https://codeclimate.com/github/octoblu/meshblu-core-job-manager/badges/coverage.svg)](https://codeclimate.com/github/octoblu/meshblu-core-job-manager)
[![npm version](https://badge.fury.io/js/meshblu-core-job-manager.svg)](http://badge.fury.io/js/meshblu-core-job-manager)
[![Gitter](https://badges.gitter.im/octoblu/help.svg)](https://gitter.im/octoblu/help)


# Force logging of particular UUIDs

Add the uuid to the redis set specified in `overrideKey`. (defaults to `override-uuids`)

```
redis-cli SADD meshblu:override-uuids b3ad254f-0ef1-401b-9c1b-22c429a60208
```
