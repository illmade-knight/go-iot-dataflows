## Deploy services

we have a microservices repo organized as follows

/root
    go.mod
    /builder
        /bigquery
    /dataflow
        /cmd
            /bigquery
                bigquerymain.go
            /enrichment
                enrichmentmain.go
            /icestore
                icestore.go
            /ingestion
                ingestionmain.go
            /servicedirector
                rundirector.go
        /e2e
            fullflowe2e_test.go

the fullfowe2e_test.go demonstrates the full dataflow we want to create in the cloud
to do this we must first create a yaml for the servicedirector microservice and deploy it
servicedirector sets up all the resources required by the other microservices in order to run

the other microservices can be deployed in any order

we want to deploy to google cloud run

our aim is to create a repeatable deployment strategy preferably with golang creating the deploy using templates etc... 
infrastructure as as service

one issue is that our repo structure, which makes our testing easy, runs from a single root go.mod file which may
create issues with a straight call to gcloud run deploy

can you first help us design a strategy for accomplishing this aim?
