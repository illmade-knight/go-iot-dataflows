## repo outline

github.com/illmade-knight/go-iot-dataflows is a repo we are developing in golang 1.24

we log using github.com/rs/zerolog v1.34.0

we prefer short definitions in separate files to lumping large amounts of code together in single files

the overall purpose of the repo is to create microservices connected by messaging

we group messaging and microservices into conceptual units called dataflows.

An example dataflow (call it "dataflow-example-1" has the following services and messaging
* microservice 1: takes mqtt messages from an external broker and adds to a pubsub topic: "ingestion"
* microservice 2: reads from the ingestion topic and adds message source metadata from a cache the places enriched messages on a topic: "enriched"
* microservice 3: reads from the enriched topic and places messages into a Bigquery style store for analysis
* microservice 4: reads from a separate subscription, also on the the enriched topic, and stores message data in long term storage.

### builder outline

github.com/illmade-knight/go-iot-dataflows/builder is used to store reusable configuration and service code for direct use by
microservices

### dataflows outline

github.com/illmade-knight/go-iot-dataflows/dataflow has separate directories used to create the concrete collection of microservices 
and resource management necessary for complete dataflow deployments

