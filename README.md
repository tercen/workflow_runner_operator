
# Workflow Runner

Set of scripts to test workflows against previously ran versions of it. 


### List of Parameters

#### Connection & Credentials 

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
|-----------------------------------------|
| user | test | Tercen username |
| passw | test | Tercen password |
| authToken | - | Authorization token to be used instead of password |
| serviceUri | http://127.0.0.1 | Tercen URI where the runner will execute the tests |
| servicePort | 5400 | Tercen instance port |


#### Template & Golden Standard

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
|-----------------------------------------|
| templateRepo | - | Github template repository (e.g. tercen/workflow_lib)  |
| templateVersion | latest | Template version or commit id |
| templatePath | - | Template path, up to filename (e.g. data/template.zip) |
| gsRepo | - | Github golden standard repository (e.g. tercen/workflow_lib)  |
| gsVersion | latest | golden standard version or commit id |
| gsPath | - | golden standard path, up to filename (e.g. data/gs.zip) |


#### Other parameters

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
|-----------------------------------------|
| filename | - | Name of a file stored in Tercen to be passed to Table Steps. *  |
| filemap | - | Local JSON file mapping TableStep ids and Tercen file Ids. *  |
| verbose | - | Switch. If present, print information messages.  |

\* If neither a filename nor filemap are provided, a file with the same name as the TableStep will be used.


### Example Calls

#### Test a template against a golden standard

```Bash
 python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip 
```






    