TODO: for future feature list for feather flow
1. when we parse instad of making a lower set of code we should know if the object is case sensitive which varies on the dialect but usually when its quoted it means that its case senssative. Snowflakes defaul is upper case but I know postgres defaults to lower case
2. At some point we will have to write code that will ping the database to get schema information and that should be hidden behind a flag
3. I don't think we need a analyze, validate, compile and parse command. Im thinking we can simplify this but I need to think this through since I know I want to add a meta reporting feature to this code base. 
4. I really need to think through how I want classifications to work since this will likly impact how the meta linting/reporting database will work
5. I really need to redo the sample_model repo and take a hard look at it to make sure that its working how I would expect
6. I don't think seeds need to be their own path and that based on the strict directory rules that we enforce feather flow should be smart enough to know how to handle it. 
7. The vs code extension needs some love and looks awful. I will at least need to build better views on it and offer a table lineage drop down vs a column level lineage drop down at least for the mvp of this product
8. I really need to rework how all of the materilizations work and make them a lot more advanced
9. Im not sure what Ill need the meta config tag for yet but I know ill want it in some capacity
10. We need to make the query comment piece more sophisticated and even offer some level of telemetry to output at runtime but I don't think that will be required for now.
11. I need to weigh if supporting many repos or even the concept of packages and if that still makes sense in this new world. 
12. Be the first tool to truly support many dialects where we're backed by apache iceberg
13. We have this ultimate tool so now we can use this to build the best data agent that exists in the game
14. every node should have schema validatoin cause it epxplains how it interactins with other nodes and it also allows us to d
15. support docker image nodes that execute things but supply the schema validatoin so you can run the static analyzer on it. 