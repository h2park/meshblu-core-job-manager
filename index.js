require('coffee-script/register');
var JobManagerRequester = require('./src/requester');
var JobManagerResponder = require('./src/responder');
module.exports = {
  JobManagerRequester: JobManagerRequester,
  JobManagerResponder: JobManagerResponder
}
