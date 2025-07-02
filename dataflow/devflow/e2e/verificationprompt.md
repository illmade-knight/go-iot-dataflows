## Hard to get testing right

gemini was having real problems with these tests - prompting it was hard

we came up with a prompt that it seemed to understand:

### Prompt

OK sorry I realize there has been an error in the logical sequence that crept into the code earlier than I had thought. 

Lets just create a new small file to try out the sequence I want 
and lets talk through how it will work. Can we work on a simple file for our ideas. 

We want a verification function that we can set up early on with a pubsub subscription listening for messages. 
This function does not know how many messages it will receive yet. 
Then we run loadgen - 
we wait for loadgen to finish and then we should know how many messages it really sent - 
now we want to signal to the verifier the loadgen has finished AND how many messages were really sent - 

This might seem over complicated but in bigger dataflows we want to be able to look for any possible area where problems come from. 
Can you take a look at this - and before creating this mini test give me your opinion on the sequence of startup, 
the signalling invovled and how we will implement it?