### prompt to new client
I have an end-to-end test that works with a servicemanager lib to setup and teardown resources. 
I want you to take a look at it. 

We're now going to create a more complex dataflow that enriches the messages with data from a devicecache - the 
device cache is fronted by redis but can be backed by different permanent databases - 
I've included an integration test that demonstrates this. 

Finally I've included the enrichment service that we'll be adding to the new dataflow - 
take a look and evaluate only - next I'll show you a new microservice we're adding for enrichment using the enrichemntservcie lib

#### included
biqquery [end to end](https://github.com/illmade-knight/go-iot-dataflows/tree/main/gardenmonitor/teste2e/pubsubbigquery/managedload) test 

relevant lib files from [servicemanager](https://github.com/illmade-knight/go-iot/tree/main/pkg/servicemanager)

### follow up 

Ok here's the new enrichment service. 

Using the patterns from our previous end to end test we want to create a new one where before we send the data to the bigquery service 
we enrich it with device metadata so we can store that in the bigquery rows for querying. 
Can you start to create this new end to end test for me?

#### included

the new enrichmenet service