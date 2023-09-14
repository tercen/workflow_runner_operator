import os 
import sys
import json

import copy

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



def get_installed_operator(client, installedOperators, opUrl, opVersion, verbose=False):
    opTag = '{}@{}'.format(opUrl, opVersion)
    comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]


    if not np.any(comp):
        # install the operator
        msg("Installing {}".format(opTag), verbose)
        installTask = CreateGitOperatorTask()
        installTask.state = InitState()
        installTask.url.uri = opUrl
        installTask.version = opVersion
        
        installTask.testRequired = False
        installTask.isDeleted = False
        installTask.owner = 'test'

        installTask = client.taskService.create(installTask)
        client.taskService.runTask(installTask.id)
        installTask = client.taskService.waitDone(installTask.id)

        operator = client.operatorService.get(installTask.operatorId)
    else:
        operator = installedOperators[which(comp)]


    return operator
    


def update_operators(workflow, refWorkflow, operatorList, client, verbose=False):
    installedOperators = client.documentService.findOperatorByOwnerLastModifiedDate('test', '')

    # Operator Id from reference workflow might be different than the current workflow instance
    # E.g.: Reference workflow is from remote instance while the test is being running locally
    for stpIdx in range(0, len(workflow.steps)):
        stp = workflow.steps[stpIdx]
        #for stp in workflow.steps:
        #         workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
        # workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
        # workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
        # workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
        

        if stp.__class__ == DataStep:
            opUrl = stp.model.operatorSettings.operatorRef.url.uri
            opVersion = stp.model.operatorSettings.operatorRef.version

            #opTag = '{}@{}'.format(opUrl, opVersion)
            #comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

            # FIXME DEBUG from here
            operator = get_installed_operator(client, installedOperators, opUrl, opVersion)



            workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version



    # Gets the required operators for the update (install them if necessary)
    for op in operatorList:
        #opTag = '{}@{}'.format(op["operatorURL"], op["version"])
        #comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

        operator = get_installed_operator(client, installedOperators, op["operatorURL"], op["version"])

        # if not np.any(comp):
        #     # install the operator
        #     msg("Installing {}".format(opTag), verbose)
        #     installTask = CreateGitOperatorTask()
        #     installTask.state = InitState()
        #     installTask.url.uri = op["operatorURL"]
        #     installTask.version = op["version"]
            
        #     installTask.testRequired = False
        #     installTask.isDeleted = False
        #     installTask.owner = 'test'

        #     installTask = client.taskService.create(installTask)
        #     client.taskService.runTask(installTask.id)
        #     installTask = client.taskService.waitDone(installTask.id)

        #     operator = client.operatorService.get(installTask.operatorId)
        # else:
        #     operator = installedOperators[which(comp)]

        stpIdx = which([op["stepId"] == stp.id for stp in refWorkflow.steps])
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow


def create_test_workflow(client, refWorkflow, workflowInfo, verbose=False):
    if refWorkflow == None:
        refWorkflow = client.workflowService.get(workflowInfo["workflowId"])
        workflow = client.workflowService.copyApp(refWorkflow.id, refWorkflow.projectId)
    else:
        workflow = copy.deepcopy(refWorkflow)

    # READ list of operators from input json and update accordingly in the cloned workflow
    if hasattr(workflowInfo, "operators"):
        operatorList = workflowInfo["operators"]
    else:
        operatorList = []

    msg("Copying workflow", verbose)
    
    # CLONE reference workflow (but doesn't create a new one just yet)
    

    workflow.name = "{}_{}".format(refWorkflow.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    workflow = update_operators(workflow, refWorkflow, operatorList, client)
    
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
    workflow = client.workflowService.create(workflow)

    return [workflow,refWorkflow]

def __file_relation(client, fileId):
    try:
        fileDoc = client.projectDocumentService.get(fileId)

        isRefRel = False
        if fileDoc.nRows == 0:
            fSchema = client.tableSchemaService.get(fileDoc.relation.id, useFactory=True)
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
        df = pl.DataFrame({"documentId":fileId})
        rr = InMemoryRelation()
        rr.inMemoryTable = utl.dataframe_to_table(df)[0]

    return rr


def __get_file_id(client, user, tbf):

    if "fileId" in tbf:
        return tbf["fileId"]
    else:
        docs = client.projectDocumentService.findSchemaByOwnerAndLastModifiedDate(user, "")
        docComp = [doc.name == tbf["filename"] for doc in docs]
        if len(docs) == 0 or not np.any(docComp):
            raise FileNotFoundError("!!ERROR!! Document {} not found. Cannot set TableStep, so aborting execution.".format(tbf["filename"]))
        idx = which(docComp)

        # TODO Abort if filename does not exist
        if isinstance(idx, list):
            doc = docs[idx[0]]
        else:
            doc = docs[idx]

        return doc.id

# Separate function for legibility
def update_table_relations(client, refWorkflow, workflow, filemap, user, verbose=False):
    msg("Setting up table step references in new workflow.", verbose)
    if refWorkflow == None:
        refWorkflow = client.workflowService.get(workflowInfo["workflowId"])

    #tableStepFiles = workflowInfo["tableStepFiles"]
    if isinstance(filemap, str): #len(tableStepFiles) == 1 and tableStepFiles[0]["stepId"] == "":
        for i in range(0, len(workflow.steps)):
            filemap = {"filename":filemap}
            if isinstance(workflow.steps[i], TableStep):
                #print( tableStepFiles[0])
                fileId = __get_file_id(client, user, filemap)
                rr = __file_relation(client, fileId)
                workflow.steps[i].model.relation = rr
                workflow.steps[i].state.taskState = DoneState()
    else:
        for tbf in filemap:
            
            tblStepIdx = which([stp.id == tbf["stepId"] for stp in workflow.steps])
            if not (isinstance(tblStepIdx, int) or len(tblStepIdx) > 0):
                continue

            fileId = __get_file_id(client, user, tbf)
            rr = __file_relation(client, fileId)
           

            workflow.steps[tblStepIdx].model.relation = rr
            workflow.steps[tblStepIdx].state.taskState = DoneState()

    client.workflowService.update(workflow)
    
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



