ok that's a great start - 
I'm going to refactor it as follows - 
The overall repo I'm creating this in is github.com/illmade-knight/go-iot-dataflows
(so thats a different repo from the github.com/illmade-knight/go-iot repo where I take the pkg files from which we keep more as a reuseable lib). 

In the repo we're doing this work in I've created a builder directory and put the service.go interface file you created in. 
Then in builder we create a separate directory for each services app and config files.

We'll create the actual microservices themselves in a dataflows directory, some of the microservices will be generally 
reusable but others will be tied to specific message types, what enrichment occurs etc. 
We'll come back to the actual running services with main() slightly later.

## Service Director
Also we'll put the servicemanager files you created in a directory under builder called
servicedirector (this allows us to import from the pkg/servicemonitor without any name confusion)

our plan for orchestrating services is that we run it first, 
before any other services are deployed in our project.

at the moment it runs from a yaml file but we've designed it so in the near future it can store that information in a more
flexible form - probably we will use firestore initially for this.

We want the other services on startup to actually be able to get their config from servicedirector 
(with a fallback to local config as we have it now)

### some general questions
can the service director/manager/monitor use our service interface.
can we unify the config file a bit?

also can you give feedback on these decisions?

