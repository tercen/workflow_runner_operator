
# Workflow Runner

Set of scripts to test templates against their golden standard runs. 


### List of Parameters

#### Connection & Credentials 

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| user | test | Tercen username |
| passw | test | Tercen password |
| authToken | - | Authorization token to be used instead of password |
| gitToken | - | Authorization token to be used when connecting to github  |
| serviceUri | http://127.0.0.1:5400 | Tercen URI where the runner will execute the tests |



#### Template & Golden Standard

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| templateRepo | - | Github template repository (e.g. tercen/workflow_lib)  |


#### Other parameters

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| verbose | - | Switch. If present, print information messages.  |


### Example Calls

#### Test a template against a golden standard

```Bash
 docker run --net=host template_tester:0.0.1 --templateRepo=tercen/git_project_test  --gitToken=ghp_xxx serviceUri = 'http://127.0.0.1:5400'
```






    