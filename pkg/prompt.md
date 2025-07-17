We have a microservices library that is used by individual microservice instances - 
I've included a few of those microservices here. 

We have an infrastructure as code type servicemanager that we use to create and teardown resources. 
We want to unify the form and creation of our actual microservices that we deploy and we want to develop the servicemanager lib to not just create resources but, 
through a service available within a google project, 
help monitor message traffic and visualize our dataflows. 

I've included an end 2 end test that demonstrates some of what we have so far - 
the use of servicemanager to ensure the microservices have the correct resources available. 
Can you help me structure and code the next stages of our microservice architectures development.