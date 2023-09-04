import os 
import sys, getopt, glob, shutil, subprocess
import json
import polars as pl

from zipfile import ZipFile



sys.path.append('./')
sys.path.append('../../')

from util import msg 
from workflow_setup_client import create_test_workflow, update_table_relations
from workflow_compare_client import diff_workflow
from workflow_stats import stats_workflow

from tercen.client.factory import TercenClient
from tercen.client import context as tercen
from tercen.model.base import RunWorkflowTask, InitState, DoneState, Workflow


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
                                "serviceUri="])
    
    # NOTE
    # When updating the golden standard it is necessary to update the IDs in the configuration file
    # (TableSteps, Operator <=> DAtaStep, etc)
    #/tercen/base/BaseObject.py

    # TODO
    # 1 - Test comparison
    # 2 - Try the full run getting workflow from github (will need to download file using wget from python)

    # python3 runner.py --templateRepo=tercen/workflow_runner --templateWkfPath=workflow_files/reference_workflow.zip --templateWkfVersion=a442105f74371285c49572148deb024436176ef8
    # wget -o /tmp/some_workflow.zip https://github.com/tercen/tercen_python_client/raw/0.7.11/setup.py

    
    workflowId = ''
    workflowVersion = ''
    serviceUri = ''
    templateRepo = ''
    templateWkfPath = ''
    #print(opts)
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

        if opt == '--serviceUri':
            serviceUri = arg

        if opt == '--workflowId':
            workflowId = arg

    #-O ./data/some_workflow.zip
    #https://github.com/tercen/workflow_runner/blob/a442105f74371285c49572148deb024436176ef8/workflow_files/reference_workflow.zip
    gitCmd = 'https://github.com/{}/raw/{}/{}'.format(templateRepo,workflowVersion,templateWkfPath)
    
  
    subprocess.call(['wget', '-O', './data/some_workflow.zip', gitCmd])
    subprocess.run(["unzip", '-o', './data/some_workflow.zip'])
    


if __name__ == '__main__':

    

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    parse_args(sys.argv[1:])
    # python3 runner.py --templateRepo=tercen/workflow_runner --templateWkfPath=workflow_files/reference_workflow.zip --templateWkfVersion=a442105f74371285c49572148deb024436176ef8
    

    conf_path = os.path.join(absPath, 'env.conf')
    #json_path = os.path.join(absPath, 'workflow_files/run_all.json')
    json_path = os.path.join(absPath, 'workflow_files/simple_run.json')
    # json_path = os.path.join(absPath, 'workflow_files/diagnostic_plot.json')
    # json_path = os.path.join(absPath, 'workflow_files/debarcode_workflow.json')
    # json_path = os.path.join(absPath, 'workflow_files/gather_join2.json')
    
    
    # wget https://github.com/tercen/tercen_python_client/raw/0.7.11/setup.py
    
    with open(json_path) as f:
        workflowInfo = json.load(f) 

    if hasattr(workflowInfo, "verbose"):
        verbose = bool(workflowInfo["verbose"])
    else:
        verbose = False

    msg( "Starting Workflow Runner.", verbose )

    username = 'test'
    passw = 'test'
    conf = {}

    with open(conf_path) as f:
        for line in f:
            if len(line.strip()) > 0:
                (key, val) = line.split(sep="=")
                conf[str(key)] = str(val).strip()
    serviceUri = ''.join([conf["SERVICE_URL"], ":", conf["SERVICE_PORT"]])

    with open(json_path) as f:
        workflowInfo = json.load(f) 


    

    fileList = glob.glob("./data/*", recursive=False)
    for f in fileList:
        if os.path.isdir(f):
            shutil.rmtree(f)

    

    with ZipFile("./workflow_files/reference_workflow.zip", 'r') as zip:
        zip.extractall("./data/")


    fileList = glob.glob("./data/*", recursive=False)
    for f in fileList:
        if os.path.isdir(f):
            with open( f + "/workflow.json" ) as wf:
                wkfJson = json.load(wf)
                wkf = Workflow.createFromJson( wkfJson )
                #print(wkf.toJson())


    # ctx = tercen.TercenContext(
    #     username=username,
    #     password=passw,
    #     serviceUri=serviceUri,
    #     workflowId=workflowInfo["workflowId"])
#    ctx = tercen.TercenContext(
#        username=username,
#        password=passw,
#        serviceUri=serviceUri)
 
    client = TercenClient(serviceUri)
    client.userService.connect(username, passw)

    project = client.projectService.get("2aa4e5e69e49703961f2af4c5e000dd1")
    wkf.projectId = project.id
    wkf.acl = project.acl
    #TODO Update project information on workflow
    
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
    
    referenceSchemaPath = "data/workflow-PDPRUY/data/"
    resultDict = diff_workflow(client, workflow, refWorkflow, referenceSchemaPath, 0.0001, "relative", workflowInfo["verbose"])
    print(resultDict)
    client.workflowService.delete(workflow.id, workflow.rev)
    """   
    stats =  stats_workflow(ctx, workflow, refWorkflow, verbose=False)
    


    print(stats)
    
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
    """