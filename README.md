Project containing:
 * web page monitor reporting statuses to a handler
 * handler sending the received statuses to a Kafka instance
 * Kafka consumer receiving certain topic's messages and passing them to a handler
 * handler pushing the messages to a Postgresql database

 Root level `Makefile` will recursively call both projects' specific `Makefile`s. There are targets for `lint`, `test` and `sdist`.
