import os 
import sys
import json
sys.path.append('./')
sys.path.append('../../')


from tercen.model.base import *
from tercen.client import context as tercen

import numpy as np
from fpdf import FPDF


def add_step_result(pdf, label, value):
    pdf.cell(40, 10, label + ':')
    pdf.set_font('Arial', '', 12)
    pdf.cell(10, 10, value)
    pdf.set_font('Arial', 'B', 12)
    pdf.ln()

    return pdf

if __name__ == '__main__':
    print( "Running Workflow tests")

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    conf_path = os.path.join(absPath, 'env.conf')
    json_path = os.path.join(absPath, 'workflow_files/run_all.json')


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
    
    workflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])

    
    tblStep = workflow.steps[0]
    project = ctx.context.client.projectService.get(workflow.projectId)
    fileList = ctx.context.client.projectDocumentService.findProjectObjectsByFolderAndName(project.name, '')
    
    for f in fileList:
        #c03_simple_100_10000.tsv
        if f.projectId == project.id and f.name == 'Crabs Data.csv':
        # if f.projectId == project.id and f.name == 'c03_simple_100_10000.tsv': 
            wkfFile = f

    fSchema = ctx.context.client.tableSchemaService.get(wkfFile.id, useFactory=True)
    
    rr = RenameRelation()
    rr.inNames = [f.name for f in fSchema.columns]
    rr.outNames = [f.name for f in fSchema.columns]
    rr.relation = SimpleRelation()
    rr.relation.id = fSchema.id

    tblStep.model.relation = rr

    tblStep.state.taskState = DoneState()

    workflow.steps[0] = tblStep
    ctx.context.client.workflowService.update(workflow)
    # END of file selection


    runTask = RunWorkflowTask()
    runTask.state = InitState()
    runTask.workflowId = workflow.id
    runTask.workflowRev = workflow.rev
    runTask.owner = project.acl.owner
    runTask.projectId = project.id

    runTask = ctx.context.client.taskService.create(obj=runTask)
    ctx.context.client.taskService.runTask(taskId=runTask.id)
    runTask = ctx.context.client.taskService.waitDone(taskId=runTask.id)
    # # Workflow executed
    
