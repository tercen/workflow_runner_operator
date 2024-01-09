
# Workflow Runner

Set of scripts to test templates against their golden standard runs. 

### Template & Golden Standard Workflow Setup

##### 1. Create the Template Repo

Create a new project based on the [template repository](https://github.com/tercen/template_workflow).

##### 2. Import the repository into Tercen

Create a new project from Github.

<img src="./documentation/img/001_NewProject.png" alt="New Project" width="500"/>

<img src="./documentation/img/002_NewProject_FromGit.png" alt="New Project from Git" width="500"/>


##### 3. Create the Template Workflow

Create a standard [Tercen workflow](https://tercen.github.io/tercen-book/workflow-and-steps.html). Run the steps as needed.

A finalized workflow might look like the one below.

<img src="./documentation/img/004_RunWorkflow.png" alt="Completed Workflow" width="300"/>


##### 4. Clone and Rename

Clone the workflow to create the **golden standard**. Rename it with an unique **_gs** suffix (see naming convention). 

<img src="./documentation/img/005_Clone.png" alt="Clone Workflow" width="600"/>

<img src="./documentation/img/006_CloneName.png" alt="Clone Workflow Name" width="600"/>

Once cloned, both Template and Golden Standard will be in the same folder. We want to move the Golden Standard workflow to a folder named **tests**. To do so, rename the Golden Standard, adding _tests/_ before the name and pressing the Ok button.

<img src="./documentation/img/007_CloneReName.png" alt="Rename" width="600"/>

<img src="./documentation/img/008_CloneReNameFolder.png" alt="Rename" width="600"/>

##### 5. Reset the Template Workflow

Open the Template Workflow (the one without the _gs** suffix), reset and save it.

##### 6. Commit the Changes to Github

Finally, select the Git button.

<img src="./documentation/img/012_Git.png" alt="Git Function" width="600"/>

Then, add any desired commit message, your personal Github token and press Ok.

<img src="./documentation/img/013_Commit.png" alt="Git Function" width="600"/>

## Naming Convention

A Template workflow is compared to its Golden Standard workflows based on a naming convention. 

A workflow is considered a Golden Standard if its name ends with **_gs***, where * is a set of letter and numbers. For example, Template_gs1, Template_gsA01 and Template_gsAA are all valid Golden Standard names, whereas Template_gs_01 is not.

A Template and a Golden Standard are considered match if they have the same *base* name, that is, everything before the _gs suffix. For example, Workflow, Workflow_gs01 and Workflow_gs02 refer to a template (Workflow) and its two Golden Standards.


## CLI Execution

The workflow runner is contained within a docker and can be called like the example below, containing all mandatory (in bold) and optional arguments:

<pre style="background-color:#DDD5">
<span style="color:#00A">docker</span> run -t --net=host tercen/workflow_runner:latest   
<b>--templateRepo=tercen/git_project_test</b>
 --branch=main --tag=1.0.0 <b>--gitToken=token</b>
 <b>[--taskId='' --token='' | --user='test' --passw='']</b>
 --serviceUri=http://127.0.0.1:5400 --opMem="500000000" 
 --toleranceType=relative --tolerance=0.001
 --update_operator --quiet --report
</pre>

#### Connection & Credentials 

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| user | test | Tercen username |
| passw | test | Tercen password |
| token | - | Authorization token to be used instead of password |
| gitToken | - | Authorization token to be used when connecting to github  |
| serviceUri | http://127.0.0.1:5400 | Tercen URI where the runner will execute the tests |



#### Template & Golden Standard

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| templateRepo | - | Github template repository (e.g. tercen/workflow_lib)  |
| branch | main | Github template repository branch  |
| tag | - | Tag or commit hash string  |


#### Other parameters

| PARAMETER | DEFAULT VALUE | DESCRIPTION |
| ---------- | ---------- |  ---------- |  
| quiet | - | Switch. If present, suppress information messages.  |
| update_operator | - | Switch. If present, check and updates operator version by latest release.  |
| report | - | Switch. If present, runs in report mode (see below).  |






    