# go-iot-dataflows
dataflows for iot using go-iot

we're moving code out of its original home in (ai-power-mvp)[https://github.com/illmade-knight/ai-power-mvp]

our first dataflow is for a [gardenmonitor](gardenmonitor)

dataflows should follow the same structure with code and deployment for each microservice

if the deployment is to google cloud unified deployment code will be in a gcpdeploy directory

end to end tests in a teste2e directory should verify all expected behaviour along with load tests for the dataflows

### next steps

we'll clean up the look of the individual services and work on making gcpdeploy work with dataflows 
as at the moment we've just got it working for one of our dataflows

next we want the microservices to work with servicemanager running as a service so that at 
startup they can check the resources 

the servicemanager should then start to give information on dataflows and we'll look at building a
frontend for those visualizations

### next, next step

this should be microservice creation etc - the 'dataflows' we want are currently demonstrated in
the end to end tests - so any individual device/organizations dataflows will end up in their own repos or
as examples
