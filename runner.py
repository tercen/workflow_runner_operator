import os 
import sys, getopt
import json
import polars as pl
import numpy as np


import string, random

#sys.path.append("../")
#sys.path.append("./")

#from util import msg, filter_by_type
#from workflow_setup_client import setup_workflow 
#from workflow_compare_client import diff_workflow

import workflow_funcs.workflow_setup as workflow_setup, \
    workflow_funcs.workflow_compare as workflow_compare, \
        workflow_funcs.util as util

#TODO Try workflow with documentId, readFCS
from tercen.client.factory import TercenClient

from tercen.model.impl import RunWorkflowTask, InitState,  Workflow, Project, GitProjectTask, Schema

class WorkflowComparisonError(Exception):
    pass

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
                               ["templateRepo=", "gitToken=", "tag=", "branch=",
                                "update_operator=", "quiet",
                                "serviceUri=", "user=", "passw=", "authToken=",
                                 "tolerance=", "toleranceType=", "taskId=" ]
                                )

    
    

    #docker run --net=host template_tester:0.0.1 --templateRepo=tercen/git_project_test  --gitToken=ddd serviceUri = 'http://127.0.0.1:5400'
    # FIXME DEBUG
    templateRepo = None #"tercen/git_project_test" #None
   

    params["user"] = 'test'
    params["passw"] = 'test'
    params["authToken"] = ''
    gitToken = None
    params["verbose"] = True
    params["tag"] = ''
    params["branch"] = 'main'

    params["update_operator"] = False
    
    params["tolerance"] = 0.001
    params["toleranceType"] = "relative"

    params["taskId"] = None

    params["serviceUri"] = "http://127.0.0.1:5400"

    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateRepo':
            templateRepo = arg

        if opt == '--gitToken':
            gitToken = arg

        if opt == '--serviceUri':
            params["serviceUri"] = arg

        if opt == '--user':
            params["user"] = arg
        
        if opt == '--passw':
            params["passw"] = arg
        
        if opt == '--authToken':
            params["authToken"] = arg

        if opt == '--tolerance':
            params["tolerance"] = float(arg)

        if opt == '--toleranceType':
            params["toleranceType"] = arg

        if opt == '--tag':
            params["tag"] = arg

        if opt == '--branch':
            params["branch"] = arg

        if opt == '--quiet':
            params["verbose"] = False

        if opt == '--taskId':
            params["taskId"] = arg

        if opt == params["update_operator"]:
            params["update_operator"] = arg

    
    client = TercenClient(params["serviceUri"])
    client.userService.connect(params["user"], params["passw"])

    params["client"] = client
   
    templateRepo = "https://github.com/" + templateRepo

    params["templateRepo"] = templateRepo
        
    if gitToken == None and "GITHUB_TOKEN" in os.environ:
        gitToken = os.environ["GITHUB_TOKEN"]

    params["gitToken"] = gitToken

    # python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip --projectId=2aa4e5e69e49703961f2af4c5e000dd1
    return params


def run(argv):
    params = parse_args(argv)
    client = params["client"]
    
    
    if params["taskId"] != None:
        # TODO Run as operator
        pass
    
    try:
        # Create temp project to run tests
        project = Project()
        project.name = 'template_test_' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
        project.acl.owner = params['user']
        project = client.projectService.create(project)
        params["projectId"] = project.id

        # Clone the template project from git
        importTask = GitProjectTask()
        importTask.owner = params['user']
        importTask.state = InitState()

        importTask.addMeta("PROJECT_ID", project.id)
        importTask.addMeta("PROJECT_REV", project.rev)
        importTask.addMeta("GIT_ACTION", "reset/pull")
        importTask.addMeta("GIT_PAT", params["gitToken"])
        importTask.addMeta("GIT_URL", params["templateRepo"])
        
        importTask.addMeta("GIT_BRANCH",params["branch"])
        importTask.addMeta("GIT_MESSAGE", "")
        importTask.addMeta("GIT_TAG", params["tag"])


        importTask = client.taskService.create(importTask)
        client.taskService.runTask(importTask.id)
        importTask = client.taskService.waitDone(importTask.id)
        
        objs = client.persistentService.getDependentObjects(project.id)
        workflowList = util.filter_by_type(objs, Workflow)


        verbose = params["verbose"]
        resultList = []

        allPass = True
        for w in workflowList:
            
            wkfName = w.name

            # FIXME DEBUG
            #if not wkfName.startswith("Complex"):
            # if wkfName != "WizardWkf":
            #     continue
                
            
            nameParts = wkfName.split("_")
            if not (nameParts[-1].startswith("gs") and len(nameParts) > 1):
                wkf = w
                gsWkf = None
                for w2 in workflowList:
                    nameParts = w2.name.split("_")
                    if w2.name == (wkfName + "_" + nameParts[-1]):
                        gsWkf = w2

                        
                        util.msg( "Testing template {} against {}.".format(wkfName, gsWkf.name ), verbose )
                        
                        workflowRun = workflow_setup.setup_workflow(client, wkf, gsWkf=gsWkf, params=params)
                    

                        util.msg("Running all steps", verbose)
                        run_workflow(workflowRun, project, client)
                        util.msg("Finished", verbose)

                        # Retrieve the updated, ran workflow
                        workflowRun = client.workflowService.get(workflowRun.id)


                        resultDict = workflow_compare.diff_workflow(client, workflowRun, gsWkf,  params["tolerance"],
                                                params["toleranceType"], verbose)


                        if len(resultDict) > 0:
                            resultList.append({w2.name: resultDict[0]})   
                            allPass = False
                            util.msg("{} and {} comparison FAILED".format(\
                                wkfName, gsWkf.name), verbose)
                        else:
                            util.msg("{} and {} comparison was SUCCESSFUL".format(\
                                wkfName, gsWkf.name), verbose)

        if allPass == False:
            with open('test_results.json', 'w', encoding='utf-8') as f:
                json.dump(resultList, f, ensure_ascii=False, indent=4)     
            raise WorkflowComparisonError

    except Exception as e:
        if type(e) == WorkflowComparisonError:
            # Runner executed succesfully, but workflow comparison failed
            # Pass the failure to github action, so the GA workflow fails
            raise

        util.msg("Workflow runner failed with error: ", True)
        util.msg(e.with_traceback(), True)

        with open('test_results.json', 'w', encoding='utf-8') as f:
            json.dump({"Failure":e.with_traceback()}, f, ensure_ascii=False, indent=4)
        
    finally:
        if project != None and client != None:
            client.workflowService.delete(project.id, project.rev)



if __name__ == '__main__':
    #absPath = os.path.dirname(os.path.abspath(__file__))
    run(sys.argv[1:])
