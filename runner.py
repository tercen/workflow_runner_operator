import os 
import sys, getopt, glob, shutil, subprocess
import json
import polars as pl

from zipfile import ZipFile

import tempfile, string, random

sys.path.append('./')
sys.path.append('../../')

from util import msg, which
from workflow_setup_client import create_test_workflow, update_table_relations
from workflow_compare_client import diff_workflow
from workflow_stats import stats_workflow

from tercen.client.factory import TercenClient
from tercen.client import context as tercen

from tercen.model.base import RunWorkflowTask, InitState, DoneState, Workflow, TableSchema


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
    opts, args = getopt.getopt(argv,"",
                               ["templateWkfId=", "templateWkfVersion=", 
                                "templateRepo=", "templateWkfPath=",
                                "serviceUri=", "projectId=",
                                "user=", "passw=", "authToken=", "dataset=", "datasetMap="])
    
    # TODO
    # python3 runner.py --templateRepo=tercen/workflow_runner --templateWkfPath=workflow_files/reference_workflow.zip --templateWkfVersion=a442105f74371285c49572148deb024436176ef8
    # wget -o /tmp/some_workflow.zip https://github.com/tercen/tercen_python_client/raw/0.7.11/setup.py

    # python3 template_tester.py  --templateRepo=tercen/workflow_lib_repo --templateVersion=latest 
    # --templatePath=template_mean_crabs_2.zip --gsRepo=tercen/workflow_lib_repo 
    # --gsVersion=latest --gsPath=golden_standard_mean_crabs_2.zip 


    # If this is passed, use this to get the workflow
    workflowId = ''
    workflowVersion = ''
    serviceUri = 'http://127.0.0.1'
    servicePort = '5400'
    templateRepo = ''
    templateWkfPath = ''
    projectId = ''
    user = 'test'
    passw = 'test'
    authToken = ''
    confFilePath = ''
    dataset = 'Crabs Data.csv'
    datasetMap = {}
    
    for opt, arg in opts:
        if opt == '-h':
            print('runner.py ARGS')
            sys.exit()

        if opt == '--templateWkfId':
            workflowId = arg
        
        if opt == '--templateWkfVersion':
            workflowVersion = arg
        
        if opt == '--templateRepo':
            templateRepo = arg


        if opt == '--templateWkfPath':
            templateWkfPath = arg

        if opt == '--projectId':
            projectId = arg

        if opt == '--serviceUri':
            serviceUri = arg

        if opt == '--servicePort':
            servicePort = arg

        if opt == '--workflowId':
            workflowId = arg

        if opt == '--user':
            user = arg
        
        if opt == '--passw':
            passw = arg
        
        if opt == '--authToken':
            authToken = arg

        if opt == '--confFilePath':
            confFilePath = arg

    #-O ./data/some_workflow.zip
    #https://github.com/tercen/workflow_runner/blob/a442105f74371285c49572148deb024436176ef8/workflow_files/reference_workflow.zip
    gitCmd = 'https://github.com/{}/raw/{}/{}'.format(templateRepo,workflowVersion,templateWkfPath)
    
    
    # tmpDir = "{}/{}".format(tempfile.gettempdir(), ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)))
    tmpDir = "./data/"

    zipFilePath = "{}/{}".format(tmpDir, templateWkfPath.split("/")[-1])

    #os.mkdir(tmpDir)
    

    #subprocess.call(['wget', '-O', zipFilePath, gitCmd])
    #subprocess.run(["unzip", '-d', tmpDir, '-o', zipFilePath])

    zip  = ZipFile(zipFilePath)
    currentZipFolder = zip.namelist()[0]
    

    params = {}

    with open( "{}/{}/workflow.json".format(tmpDir, currentZipFolder) ) as wf:
        wkfJson = json.load(wf)
        wkf = Workflow.createFromJson( wkfJson )
        params["workflow"] = wkf
        

    params["referenceSchemaPath"] = "{}/{}/data/".format(tmpDir, currentZipFolder)


    serviceUri = '{}:{}'.format(serviceUri, servicePort)

    client = TercenClient(serviceUri)
    client.userService.connect(user, passw)

    params["client"] = client
    params["projectId"] = projectId
    params["confFilePath"] = confFilePath

    
    #FIXME Hardcoded user
    

    #print(client.projectDocumentService.findSchemaByOwnerAndLastModifiedDate("test", ""))
    docs = client.projectDocumentService.findSchemaByOwnerAndLastModifiedDate(user, "")
    idx = which([doc.name == dataset for doc in docs])
    doc = docs[idx[0]]
    params["datasetId"] = doc.id


    return params


if __name__ == '__main__':
    
    #python3 runner.py --templateRepo=tercen/workflow_runner --templateWkfPath=workflow_files/reference_workflow.zip --templateWkfVersion=a442105f74371285c49572148deb024436176ef8 --projectId=2aa4e5e69e49703961f2af4c5e000dd1

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    params = parse_args(sys.argv[1:])
    
    
    
    #conf_path = os.path.join(absPath, 'env.conf')
    #json_path = os.path.join(absPath, 'workflow_files/run_all.json')
    
    #f8a28564-fd58-453c-a3f4-1111bf315f0b
    print(params["datasetId"])
    workflowInfo = {"verbose":True, "toleranceType":"relative","tolerance":0.001,"operators":[], 
                    "tableStepFiles":[{"stepId":"", "fileId":params["datasetId"]}]}
    #with open(json_path) as f:
    #    workflowInfo = json.load(f) 

    if hasattr(workflowInfo, "verbose"):
        verbose = bool(workflowInfo["verbose"])
    else:
        verbose = False

    msg( "Starting Workflow Runner.", verbose )

    client = params["client"]

    project = client.projectService.get(params["projectId"])
    wkf = params["workflow"]
    wkf.projectId = project.id
    wkf.acl = project.acl

    
    workflows = create_test_workflow(client, wkf, workflowInfo, verbose=workflowInfo["verbose"])
    workflow = workflows[0]
    refWorkflow = workflows[1]

    update_table_relations(client, refWorkflow, workflow, workflowInfo, verbose=workflowInfo["verbose"])

    msg("Running all steps", workflowInfo["verbose"])
    
    #refWorkflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])
    run_workflow(workflow, project, client)
    msg("Finished", workflowInfo["verbose"])
    
    # Retrieve the updated, ran workflow
    workflow = client.workflowService.get(workflow.id)
    

    resultDict = diff_workflow(client, workflow, refWorkflow, params["referenceSchemaPath"], workflowInfo["tolerance"],
                                workflowInfo["toleranceType"], workflowInfo["verbose"])
    print(resultDict)
    client.workflowService.delete(workflow.id, workflow.rev)

    #stats =  stats_workflow(ctx, workflow, refWorkflow, verbose=False)
    


    #print(stats)
    
    # if workflowInfo["updateOnSuccess"] == "True" and len(resultDict) == 0:
    #     print("Updating reference workflow")
    #     TODO Maybe
    #     ctx.context.client.workflowService.delete(workflow.id, workflow.rev)
    #     refWorkflow = update_operators(refWorkflow, operatorList, ctx)
    #     for stp in refWorkflow.steps[1:]:
    #         TODO Update based on step type
    #         stp.state.taskState = InitState()
        

    #     ctx.context.client.workflowService.update(refWorkflow)
    #     run_workflow(refWorkflow, project, ctx)
    