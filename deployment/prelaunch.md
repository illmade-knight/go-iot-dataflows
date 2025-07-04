can I just query the order of creation and provision - 
i'm just checking the order is how I would have expected - feel free to correct any misconceptions - 

1) 1st we create and deploy servicedirector this will probably be with the default service account since it will need access to all the apis used 
2) (maybe we should preflight that this service account DOES have permissions to create all the necessary resources). 
3) We should now have a reference to the URL of the deployed servicedirector as this is needed by the other services on startup as an environment variable. 
4) Next we create the service accounts for the services we are about to create - these should only have the necessary permissions for their resources. 
5) Finally we create services and deploy them.

We do not teardown things now - we will go off and look at the services and IAM created 

(can we have a nicely formatted output from the deployment phase). 
After we should be able to call the same main with the teardown flag. 
How close to this are we at the moment.