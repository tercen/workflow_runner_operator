import os 
import sys
import json

import uuid

from datetime import datetime

sys.path.append('./')
sys.path.append('../../')


from util import msg, which


from tercen.model.base import *
from tercen.client import context as tercen

import numpy as np
from fpdf import FPDF
from datetime import datetime

import tercen.http.HttpClientService as th
import tercen.util.helper_functions as utl
import polars as pl




def update_operators(workflow, refWorkflow, operatorList, ctx, verbose=False):
    installedOperators = ctx.context.client.documentService.findOperatorByOwnerLastModifiedDate('test', '')

    # Gets the required operators for the update (install them if necessary)
    for op in operatorList:
        opTag = '{}@{}'.format(op["operatorURL"], op["version"])
        comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

        if not np.any(comp):
            # install the operator
            msg("Installing {}".format(opTag), verbose)
            installTask = CreateGitOperatorTask()
            installTask.state = InitState()
            installTask.url.uri = op["operatorURL"]
            installTask.version = op["version"]
            
            installTask.testRequired = False
            installTask.isDeleted = False
            installTask.owner = 'test'

            installTask = ctx.context.client.taskService.create(installTask)
            ctx.context.client.taskService.runTask(installTask.id)
            installTask = ctx.context.client.taskService.waitDone(installTask.id)

            operator = ctx.context.client.operatorService.get(installTask.operatorId)
        else:
            operator = installedOperators[which(comp)]

        stpIdx = which([op["stepId"] == stp.id for stp in refWorkflow.steps])
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow


def create_test_workflow(ctx, workflowInfo, verbose=False):
    refWorkflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])

    # READ list of operators from input json and update accordingly in the cloned workflow
    operatorList = workflowInfo["operators"]

    msg("Copying workflow", verbose)
    
    # CLONE reference workflow (but doesn't create a new one just yet)
    workflow = ctx.context.client.workflowService.copyApp(refWorkflow.id, refWorkflow.projectId)

    workflow.name = "{}_{}".format(refWorkflow.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    # # TODO Might be unnecessary
    # for lnk in workflow.links:
    #     lnk.id = str(uuid.uuid4()) 

    
    # tableStepFiles = workflowInfo["tableStepFiles"]

    # # TODO Link id update might be unnecessary
    # for stp in workflow.steps:
    #     oldId = stp.id
    #     newId = str(uuid.uuid4())
    #     stp.id = newId
        

    #     tblStepIdx = which([oldId == tbf["stepId"] for tbf in tableStepFiles])
    #     if isinstance(tblStepIdx, int) or len(tblStepIdx) > 0:
    #             tableStepFiles.append( {"stepId":stp.id, "fileId":tableStepFiles[tblStepIdx]["fileId"]} )

    #     for k in range(0, len(stp.inputs)):
    #         stp.inputs[k].id = "{}-i-{:d}".format(newId, k)
    #         for lnk in workflow.links:
    #             if lnk.inputId == "{}-i-{:d}".format(oldId, k):
    #                 lnk.inputId = stp.inputs[k].id

                

    #     for k in range(0, len(stp.outputs)):
    #         stp.outputs[k].id = "{}-o-{:d}".format(newId, k)
    #         for lnk in workflow.links:
    #             if lnk.outputId == "{}-o-{:d}".format(oldId, k):
    #                 lnk.outputId = stp.outputs[k].id

    workflow = update_operators(workflow, refWorkflow, operatorList, ctx)
    
    #FIXME If nothing changes, the cached version of the computedRelation is used
    # Not usually a problem, but then we cannot delete the new workflow if needed
    # because it indicates a dependency to the reference workflow
    for stp in workflow.steps:
        if hasattr(stp, "computedRelation"):
            #TODO Check if this factor actually exists
            # Operators (like downsample) might ot have it
            yFac = stp.model.axis.xyAxis[0].yAxis.graphicalFactor
            nf = NamedFilter()
            fe = FilterExpr()
            fe.factor = yFac.factor
            fe.filterOp = 'notequals'
            fe.stringValue = '-98765456789'
            nf.filterExprs = [fe] 
            nf.logical = 'or'
            nf.isNot = False
            nf.name = yFac.factor.name
            stp.model.filters.namedFilters.append(nf)
            
        
        stp.state.taskState = InitState()


    # Create the new workflow with the required changes
    workflow = ctx.context.client.workflowService.create(workflow)

    return workflow

# Separate function for legibility
def update_table_relations(ctx, workflow, workflowInfo, verbose=False):
    msg("Setting up table step references in new workflow.", verbose)
    refWorkflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])
    tableStepFiles = workflowInfo["tableStepFiles"]
    for tbf in tableStepFiles:
        
        tblStepIdx = which([stp.id == tbf["stepId"] for stp in workflow.steps])
        if not (isinstance(tblStepIdx, int) or len(tblStepIdx) > 0):
            continue
        
        try:
            fileDoc = ctx.context.client.projectDocumentService.get(tbf["fileId"])

            isRefRel = False
            if fileDoc.nRows == 0:
                fSchema = ctx.context.client.tableSchemaService.get(fileDoc.relation.id, useFactory=True)
                isRefRel = True
            else:
                fSchema = fileDoc

            rr = RenameRelation()
            rr.inNames = [f.name for f in fSchema.columns]
            rr.inNames.append("{}._rids".format(fSchema.id))
            rr.inNames.append("{}.tlbId".format(fSchema.id))
            rr.outNames = [f.name for f in fSchema.columns]
            rr.outNames.append("rowId")
            rr.outNames.append("tableId")

            if isRefRel == True:
                rr.relation = ReferenceRelation()
                rr.relation.relation = SimpleRelation()
                rr.relation.id = fileDoc.id #fSchema.id
                rr.relation.relation.id = fSchema.id
                rr.id = "rename_{}".format(fileDoc.id)
            else:
                rr.relation = SimpleRelation()
                rr.relation.id = fileDoc.id #fSchema.id
                rr.id = "rename_{}".format(fileDoc.id)
        except:
            # If the file is used as an object (not the table operator) there will be no schema
            # So we build a dataframe with the documentId
            df = pl.DataFrame({"documentId":tbf["fileId"]})
            rr = InMemoryRelation()
            rr.inMemoryTable = utl.dataframe_to_table(df)[0]
            

        workflow.steps[tblStepIdx].model.relation = rr
        workflow.steps[tblStepIdx].state.taskState = DoneState()


    #  # Check and select Gather steps
    # # Change the following ids
    # # In table step, the rename id in model must match the relation id
    # # The gather step must refer to this new id
    # # TODO Check if necessary
    # tableIdMaps = {}
    # joinLinkMaps = {}
    # for i in range(0, len(workflow.steps)):
    #     stp = workflow.steps[i]
    #     refStp = refWorkflow.steps[i]

    #     if hasattr(stp.model, "relation") and hasattr(stp.model.relation, "relation"):
    #         tableIdMaps[refStp.model.relation.id] = i
    #         stp.model.relation.id = "{}".format(stp.model.relation.id)
            

    # # For gather steps, find the reference relation id, and use the new id
    # for i in range(0, len(workflow.steps)):
    #     # refStp = refWorkflow.steps[i]
    #     stp = workflow.steps[i]
    #     if isinstance(stp, MeltStep):
            
    #         for k in range(0, len(stp.meltedAttributes)):
    #             ma = stp.meltedAttributes[k]
    #             tableStp = workflow.steps[tableIdMaps[ma.relationId]]
    #             stp.meltedAttributes[k].relationId = tableStp.model.relation.id
                
    #     if isinstance(stp, JoinStep):
    #         for k in range(0, len(stp.rightAttributes)):
    #             ma = stp.rightAttributes[k]
    #             if not ma.relationId == '':
    #                 tableStp = workflow.steps[tableIdMaps[ma.relationId]]
    #                 stp.rightAttributes[k].relationId = tableStp.model.relation.id

    ctx.context.client.workflowService.update(workflow)
    
if __name__ == '__main__':
    print( "Running Workflow tests")

    absPath = os.path.dirname(os.path.abspath(__file__))
    
    conf_path = os.path.join(absPath, 'env.conf')
    json_path = os.path.join(absPath, 'workflow_files/run_all.json')
    # json_path = os.path.join(absPath, 'workflow_files/debarcode_workflow.json')
    # json_path = os.path.join(absPath, 'workflow_files/gather_join2.json')


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
    

    
    workflow = create_test_workflow(ctx, workflowInfo, workflowInfo["verbose"])

    update_table_relations(ctx, workflow, workflowInfo, workflowInfo["verbose"])



