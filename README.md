# qdag_rest_api
# the Id
# p_qdag must be installed in the home directory exp (/home/ubuntu/p_qdag)
# p_qdag must contains test_queries folder (it contains all the queries with => their name must be includer in the data.json file)
# modify meta_data json file in ressources/data/data.json, you must respect the schema 
# start qdag api : "./mvnw spring-boot:run" 
# Get HTTP Request :
# 1- /get-metadata => return existing database and their queries it returns a json object (see data.json in ressources/data.json)
# 2- /run-query => it must have 2 parameters: query-id , and the db folder name (~/db-name) => it returns a string (results)
