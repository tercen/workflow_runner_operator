import os 
import sys, getopt, glob, shutil, subprocess
import json
import polars as pl
import numpy as np
from zipfile import ZipFile

import tempfile, string, random

sys.path.append("../")
sys.path.append("./")

from util import msg, which, filter_by_type
from workflow_setup_client import setup_workflow, update_table_relations
from workflow_compare_client import diff_workflow

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
                               ["templateRepo=", "gitToken=",
                                "serviceUri=", "user=", "passw=", "authToken=",
                                 "tolerance=", "toleranceType=", "taskId=" ]
                                )

    
    

    #docker run --net=host template_tester:0.0.1 --templateRepo=tercen/git_project_test  --gitToken=ddd serviceUri = 'http://127.0.0.1:5400'

    templateRepo = "tercen/git_project_test"
   

    user = 'test'
    passw = 'test'
    authToken = ''
    gitToken = None
    verbose = True
    
    tolerance = 0.001
    toleranceType="relative"

    taskId = None


    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateRepo':
            templateRepo = arg

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

        if opt == '--taskId':
            taskId = arg

    
    client = TercenClient(serviceUri)
    client.userService.connect(user, passw)
    

    params["client"] = client
    params["user"] = user

    params["taskId"] = taskId


    params["verbose"] = verbose
    params["tolerance"] = tolerance
    params["toleranceType"] = toleranceType

   
    templateRepo = "https://github.com/" + templateRepo

    params["templateRepo"] = templateRepo
        
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

    # FIXME Test and Remove
    #project = client.projectService.get(params["projectId"])

    importTask = GitProjectTask()
    importTask.owner = params['user']
    importTask.state = InitState()

    importTask.addMeta("PROJECT_ID", project.id)
    importTask.addMeta("PROJECT_REV", project.rev)
    importTask.addMeta("GIT_ACTION", "reset/pull")
    importTask.addMeta("GIT_PAT", params["gitToken"])
    importTask.addMeta("GIT_URL", params["templateRepo"])
    #TODO Have it as parameter, perhaps
    importTask.addMeta("GIT_BRANCH", "main")
    importTask.addMeta("GIT_MESSAGE", "")
    importTask.addMeta("GIT_TAG", "")


    importTask = client.taskService.create(importTask)
    client.taskService.runTask(importTask.id)
    importTask = client.taskService.waitDone(importTask.id)
    
    objs = client.persistentService.getDependentObjects(project.id)
    #FIXME filter objs array by type, then find by name
    workflowList = filter_by_type(objs, Workflow)
    # schemaList = filter_by_type(objs, Schema)
    inputFileList = []

    verbose = params["verbose"]
    resultList = []
    for w in workflowList:
        
        # TODO Check conventions here actually
        wkfName = w.name
        
        nameParts = wkfName.split("_")
        if not (nameParts[-1].startswith("gs") and len(nameParts) > 1):
            wkf = w

            
            for w2 in workflowList:
                nameParts = w2.name.split("_")
                if w2.name == (wkfName + "_" + nameParts[-1]):
                    gsWkf = w2

            
            msg( "Starting Workflow Runner.", verbose )
            msg( "Testing template {} against {}.".format(wkfName, gsWkf.name ), verbose )

    

            workflowRun = setup_workflow(client, wkf, gsWkf=gsWkf, \
                                 params=params, update_operator_version=False, \
                                 verbose=verbose)
        



            msg("Running all steps", verbose)


            run_workflow(workflowRun, project, client)
            msg("Finished", verbose)

            # Retrieve the updated, ran workflow
            workflowRun = client.workflowService.get(workflowRun.id)

 
#    try:
            resultDict = diff_workflow(client, workflowRun, gsWkf,  params["tolerance"],
                                    params["toleranceType"], verbose)

            resultList.append({wkfName: resultDict})   
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
