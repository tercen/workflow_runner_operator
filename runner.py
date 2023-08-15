import os 
import sys
import json

sys.path.append('./')
sys.path.append('../../')

from util import msg 
from workflow_setup import create_test_workflow, update_table_relations
from workflow_compare import diff_workflow
from workflow_stats import stats_workflow


from tercen.client import context as tercen
from tercen.model.base import RunWorkflowTask, InitState, DoneState


def run_workflow(workflow, project, ctx):
    # RUN the CLONED workflow 
    runTask = RunWorkflowTask()
    runTask.state = InitState()
    runTask.workflowId = workflow.id
    runTask.workflowRev = workflow.rev
    runTask.owner = project.acl.owner
    runTask.projectId = project.id

    runTask = ctx.context.client.taskService.create(obj=runTask)
    ctx.context.client.taskService.runTask(taskId=runTask.id)
    runTask = ctx.context.client.taskService.waitDone(taskId=runTask.id)

if __name__ == '__main__':
    absPath = os.path.dirname(os.path.abspath(__file__))
    
    conf_path = os.path.join(absPath, 'env.conf')
    json_path = os.path.join(absPath, 'workflow_files/run_all.json')
    # json_path = os.path.join(absPath, 'workflow_files/diagnostic_plot.json')
    # json_path = os.path.join(absPath, 'workflow_files/debarcode_workflow.json')
    # json_path = os.path.join(absPath, 'workflow_files/gather_join2.json')
    
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

    ctx = tercen.TercenContext(
        username=username,
        password=passw,
        serviceUri=serviceUri,
        workflowId=workflowInfo["workflowId"])
    
    
    workflow = create_test_workflow(ctx, workflowInfo, verbose=workflowInfo["verbose"])

    update_table_relations(ctx, workflow, workflowInfo, verbose=workflowInfo["verbose"])
    
    msg("Running all steps", workflowInfo["verbose"])

    refWorkflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])
    run_workflow(workflow, ctx.context.client.projectService.get(refWorkflow.projectId), ctx)
    msg("Finished", workflowInfo["verbose"])

    # Retrieve the updated, ran workflow
    workflow = ctx.context.client.workflowService.get(workflow.id)
    resultDict = diff_workflow(ctx, workflow, refWorkflow, workflowInfo["tolerance"], workflowInfo["toleranceType"], workflowInfo["verbose"])
    print(resultDict)


    stats =  stats_workflow(ctx, workflow, refWorkflow, verbose=False)
    


    print(stats)
    ctx.context.client.workflowService.delete(workflow.id, workflow.rev)