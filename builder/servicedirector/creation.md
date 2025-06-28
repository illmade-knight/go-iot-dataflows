## ServiceDirector

we're creating github.com/illmade-knight/go-iot-dataflows/builder/servicedirector

the service leverages the github.com/illmade-knight/go-iot/pkg/servicemanager library
which helps setup and teardown cloud resources.

### Limited initial operation

servicedirector is the first service we deploy in our cloud environment.

On startup it creates or verifies all the resources used by microservices in our service architecture.
Our microservices are typically connected by a messaging architecture and we group messaging and microservices 
into conceptual units called dataflows.

An example dataflow (call it "dataflow-example-1" has the following services and messaging
* microservice 1: takes mqtt messages from an external broker and adds to a pubsub topic: "ingestion"
* microservice 2: reads from the ingestion topic and adds message source metadata from a cache the places enriched messages on a topic: "enriched"
* microservice 3: reads from the enriched topic and places messages into a Bigquery style store for analysis
* microservice 4: reads from a separate subscription, also on the the enriched topic, and stores message data in long term storage.

At startup each microservice contacts the servicedirector, identifies itself and the dataflow it is part of, 
and ensures all the cloud resources it needs are created and available.

### Purpose

In testing services can call servicemanager directly for creation and teardown of resources 
but in production microservices may have limited roles and we want a secure point of management of resources.

### Files for prompt

I've uploaded some of the github.com/illmade-knight/go-iot/pkg/servicemanager library used to create and teardown cloud resources.

our deployed services use BaseServer from service.go to provide consistency

I've uploaded existing files from the servicedirector 

### To do first

Can you look at the service.go file and the initial servicedirector files and offer some options - 
I'd first like to improve the basic setup of our services and make startup and provision of heath checks easy across 
all our services cleaner and consistent - can you start with this and then we'll move on to the actual refactor of 
servicedirector so it can provide those startup checks to microservices in dataflows?



