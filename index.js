require("coffee-script/register")
var JobManagerRequester = require("./src/requester")
var JobManagerResponder = require("./src/responder")
var JobManagerResponderDequeuer = require("./src/responder-dequeuer")

module.exports = {
  JobManagerRequester,
  JobManagerResponder,
  JobManagerResponderDequeuer,
}
