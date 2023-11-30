import os 
import sys, getopt, glob, shutil, subprocess
import json
import polars as pl
import numpy as np
from zipfile import ZipFile

import tempfile, string, random

sys.path.append("../")
sys.path.append("./")

from workflow_runner.util import msg, which
from workflow_runner.workflow_setup_client import create_test_workflow, update_table_relations
from workflow_runner.workflow_compare_client import diff_workflow

import tercen.util.helper_functions as utl


from tercen.client.factory import TercenClient

from tercen.model.impl import RunWorkflowTask, InitState,  Workflow, Project, GitProjectTask, Schema

def numpy_to_list(obj):
    if isinstance(obj, np.ndarray):
        obj = obj.tolist()

        for i in range(0, len(obj)):
            obj[i] = numpy_to_list(obj[i])

        return obj
    elif isinstance(obj, list):
        for i in range(0, len(obj)):
            obj[i] = numpy_to_list(obj[i])

        return obj
    elif isinstance(obj, dict):
        for key in obj:
            obj[key] = numpy_to_list(obj[key])
        
        return obj
    elif isinstance(obj, pl.Series):
        obj = obj.to_list()

        for i in range(0, len(obj)):
            obj[i] = numpy_to_list(obj[i])

        return obj
    else:
        return obj

def run_workflow(workflow, project, client):
    # RUN the CLONED workflow 
    runTask = RunWorkflowTask()
    runTask.state = InitState()
    runTask.workflowId = workflow.id
    runTask.workflowRev = workflow.rev
    runTask.owner = project.acl.owner
    runTask.projectId = project.id

    runTask = client.taskService.create(obj=runTask)
    client.taskService.runTask(taskId=runTask.id)
    runTask = client.taskService.waitDone(taskId=runTask.id)

def parse_args(argv):
    params = {}
    opts, args = getopt.getopt(argv,"",
                               ["templateRepo=", "templateName=", "cellranger",
                                "gsName=", "gitToken="
                                "serviceUri=", "user=", "passw=", "authToken=",
                                 "tolerance=", "toleranceType=" ]
                                )

    
    
#python3 template_tester.py  --templateRepo=tercen/scRNAseq_basic_template_test --gsRepo=templateRepo=tercen/scRNAseq_basic_template_test --gsPath=tests/example_test_gs.zip
    serviceUri = 'http://127.0.0.1:5400'

    templateRepo = None #"tercen/git_project_test"
    templateVersion = 'latest'
    templateName =  None #"Simple" 
   

    gsName = 'gs01'
    

    user = 'test'
    passw = 'test'
    authToken = ''
    gitToken = None
    verbose = False
    
    tolerance = 0.001
    toleranceType="relative"


    cellranger = False

    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        
        if opt == '--templateVersion':
            templateVersion = arg

        if opt == '--cellranger':
            cellranger = True
        
        if opt == '--templateRepo':
            templateRepo = arg


        if opt == '--templateName':
            templateName = arg

        if opt == '--gsName':
            gsName = arg

        if opt == '--gitToken':
            gitToken = arg


        if opt == '--serviceUri':
            serviceUri = arg

        if opt == '--user':
            user = arg
        
        if opt == '--passw':
            passw = arg
        
        if opt == '--authToken':
            authToken = arg

        if opt == '--tolerance':
            tolerance = float(arg)

        if opt == '--toleranceType':
            toleranceType = arg

        if opt == '--verbose':
            verbose = True


    
    if templateVersion == "latest":
        templateVersion = "main"

    client = TercenClient(serviceUri)
    client.userService.connect(user, passw)

    params["client"] = client
    params["user"] = user


    params["verbose"] = verbose
    params["tolerance"] = tolerance
    params["toleranceType"] = toleranceType

   
    templateRepo = "https://github.com/" + templateRepo

    params["templateVersion"] = templateVersion
    params["templateRepo"] = templateRepo
    params["templateName"] = templateName
    params["gsName"] = gsName
    
    params["cellranger"] = cellranger
        
    if gitToken == None and "GITHUB_TOKEN" in os.environ:
        gitToken = os.environ["GITHUB_TOKEN"]

    params["gitToken"] = gitToken

    # python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip --projectId=2aa4e5e69e49703961f2af4c5e000dd1
    return params


def run(argv):
    absPath = os.path.dirname(os.path.abspath(__file__))
    
    params = parse_args(argv)
    client = params["client"]
    

    # Create temp project to run tests
    project = Project()
    project.name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    project.name = 'template_test_' + project.name
    project.acl.owner = params['user']
    project = client.projectService.create(project)
    params["projectId"] = project.id

    project = client.projectService.get(params["projectId"])


    importTask = GitProjectTask()
    importTask.owner = params['user']
    importTask.state = InitState()

    importTask.addMeta("PROJECT_ID", project.id)
    importTask.addMeta("PROJECT_REV", project.rev)
    importTask.addMeta("GIT_ACTION", "reset/pull")
    importTask.addMeta("GIT_PAT", params["gitToken"])
    importTask.addMeta("GIT_URL", params["templateRepo"])
    importTask.addMeta("GIT_BRANCH", params["templateVersion"])
    importTask.addMeta("GIT_MESSAGE", "")
    importTask.addMeta("GIT_TAG", "")


    
    importTask = client.taskService.create(importTask)
    client.taskService.runTask(importTask.id)
    importTask = client.taskService.waitDone(importTask.id)
    
    objs = client.persistentService.getDependentObjects(project.id)

    inputFileList = []

    for o in objs:
        if isinstance(o, Workflow) and o.name == params["templateName"]:
            wkf = o

        if isinstance(o, Workflow) and o.name == params["templateName"]+"_"+params["gsName"]:
            gsWkf = o

        if isinstance(o, Schema):
            inputFileList.append(o)

    # FIXME Take DEFAULT values as parameters
    verbose = params["verbose"]

    resultList = []

    msg( "Starting Workflow Runner.", verbose )
    # msg( "Testing template {}/{}.".format(params["templateRepo"], params["templatePath"]), verbose )

    workflows = create_test_workflow(client, wkf, params, verbose=verbose)
    workflow = workflows[0]
    # templateWorkflow = workflows[1]

        

    try:
        update_table_relations(client, workflow, gsWkf, inputFileList, params["user"], params["gitToken"], verbose=verbose, cellranger=params["cellranger"])
        
    except FileNotFoundError as e:
        print(e)
        workflow.steps = []
        client.workflowService.update(workflow)
        client.workflowService.delete(workflow.id, workflow.rev)
        sys.exit(1)

    msg("Running all steps", verbose)


    run_workflow(workflow, project, client)
    msg("Finished", verbose)

    # Retrieve the updated, ran workflow
    workflow = client.workflowService.get(workflow.id)

 
#    try:
    resultDict = diff_workflow(client, workflow, gsWkf,  params["tolerance"],
                            params["toleranceType"], verbose)

    resultList.append(resultDict)
#    except Exception as e:
#        print(e)
    
        

        


    client.workflowService.delete(project.id, project.rev)
    print(resultList)

    # # Remove tmp files and zip file
    # fileList = glob.glob("{}/*".format(tmpDir), recursive=False)
    # for f in fileList:
    #     if os.path.isdir(f):
    #         shutil.rmtree(f)
    #     else:
    #         os.unlink(f)



if __name__ == '__main__':
    #absPath = os.path.dirname(os.path.abspath(__file__))
    run(sys.argv[1:])
