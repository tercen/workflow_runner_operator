import os, uuid
import sys
import json
import random, string
sys.path.append('./')
sys.path.append('../../')


import jsondiff

from tercen.model.base import *
from tercen.client import context as tercen

import numpy as np
from fpdf import FPDF
from datetime import datetime

import tercen.http.HttpClientService as th
import tercen.util.helper_functions as utl
import polars as pl





def compare_column_schemas(schema, refSchema):
    res = {}


    return res

def which(arr):
    trueIdx = []
    for idx, val in enumerate(arr):
        if val == True:
            trueIdx.append(idx)

    if len(trueIdx) == 1:
        trueIdx = trueIdx[0]
    return trueIdx

def compare_column_names(colNames, refColNames):
    results = {}

    if len(colNames) != len(refColNames):
        results["NumberOfColumns"] = "Different number of column: {:d} vs {:d} (Reference vs New)".format(len(refColNames), len(colNames))

    for i in range(0, len(refColNames)):
        refColNames[i]
        columnFound = False
        for j in range(0, len(colNames)):
            if refColNames[i] == colNames[j]:
                columnFound = True
                if i != j:
                    if "ColumnPosition" in results.keys():
                        results["ColumnPosition"].append( "{} position does not match: {:d} vs {:d} (Reference vs New)".format(refColNames[i], i, j) )
                    else:
                        results["ColumnPosition"] = [ "{} position does not match: {:d} vs {:d} (Reference vs New)".format(refColNames[i], i, j) ]
                break
        
        if not columnFound:
            if "RefColumns" in results.keys():
                results["RefColumns"].append( "Column {} from reference workflow not found.".format(refColNames[i]) )
            else:
                results["RefColumns"] = [ "Column {} from reference workflow not found.".format(refColNames[i]) ]


    for i in range(0, len(colNames)):
        columnFound = False
        for j in range(0, len(refColNames)):
            if colNames[i] == refColNames[j]:
                columnFound = True
                break

        if not columnFound:
            if "Columns" in results.keys():
                results["Columns"].append( "Column {} from new workflow not found in reference.".format(colNames[i]) )
            else:
                results["Columns"] = [ "Column {} from new workflow not found reference.".format(colNames[i]) ]

    return results

def update_operators(workflow, operatorList, ctx):
    installedOperators = ctx.context.client.documentService.findOperatorByOwnerLastModifiedDate('test', '')

    # Gets the required operators for the update (install them if necessary)
    for op in operatorList:
        opTag = '{}@{}'.format(op["operatorURL"], op["version"])
        comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

        if not np.any(comp):
            # install the operator
            print("Installing {}".format(opTag))
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
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
        workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow

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
    
    # GET Reference workflow
    refWorkflow = ctx.context.client.workflowService.get(workflowInfo["workflowId"])


    # READ list of operators from input json and update accordingly in the cloned workflow
    operatorList = workflowInfo["operators"]

    # CLONE reference workflow (but doesn't create a new one just yet)
    workflow = ctx.context.client.workflowService.copyApp(refWorkflow.id, refWorkflow.projectId)

    workflow.name = "{}_{}".format(refWorkflow.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    # NOT THE ISSUE
    for lnk in workflow.links:
        lnk.id = str(uuid.uuid4()) 

    
    # NEED to update this
    tableStepFiles = workflowInfo["tableStepFiles"]
    len(tableStepFiles)
    for stp in workflow.steps:
        oldId = stp.id
        newId = str(uuid.uuid4())
        stp.id = newId
        

        tblStepIdx = which([oldId == tbf["stepId"] for tbf in tableStepFiles])
        if isinstance(tblStepIdx, int) or len(tblStepIdx) > 0:
                tableStepFiles.append( {"stepId":stp.id, "fileId":tableStepFiles[tblStepIdx]["fileId"]} )

        for k in range(0, len(stp.inputs)):
            stp.inputs[k].id = "{}-i-{:d}".format(newId, k)
            for lnk in workflow.links:
                if lnk.inputId == "{}-i-{:d}".format(oldId, k):
                    lnk.inputId = stp.inputs[k].id

                

        for k in range(0, len(stp.outputs)):
            stp.outputs[k].id = "{}-o-{:d}".format(newId, k)
            for lnk in workflow.links:
                if lnk.outputId == "{}-o-{:d}".format(oldId, k):
                    lnk.outputId = stp.outputs[k].id


    workflow = update_operators(workflow, operatorList, ctx)

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
            nf.filterExprs = [fe] #FIXME
            nf.logical = 'or'
            nf.isNot = False
            nf.name = yFac.factor.name
            stp.model.filters.namedFilters.append(nf)
            
        
        stp.state.taskState = InitState()

    



    # Create the new workflow with the required changes
    workflow = ctx.context.client.workflowService.create(workflow)
    


    project = ctx.context.client.projectService.get(workflow.projectId)
    

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

    # Check and select Gather steps
    # Change the following ids
    # In table step, the rename id in model must match the relation id
    # The gather step must refer to this new id
    tableIdMaps = {}
    joinLinkMaps = {}
    for i in range(0, len(workflow.steps)):
        stp = workflow.steps[i]
        refStp = refWorkflow.steps[i]

        if hasattr(stp.model, "relation") and hasattr(stp.model.relation, "relation"):
            tableIdMaps[refStp.model.relation.id] = i
            stp.model.relation.id = "{}".format(stp.model.relation.id)
            

    # For gather steps, find the reference relation id, and use the new id
    for i in range(0, len(workflow.steps)):
        # refStp = refWorkflow.steps[i]
        stp = workflow.steps[i]
        if isinstance(stp, MeltStep):
            
            for k in range(0, len(stp.meltedAttributes)):
                ma = stp.meltedAttributes[k]
                tableStp = workflow.steps[tableIdMaps[ma.relationId]]
                stp.meltedAttributes[k].relationId = tableStp.model.relation.id
                
        if isinstance(stp, JoinStep):
            for k in range(0, len(stp.rightAttributes)):
                ma = stp.rightAttributes[k]
                if not ma.relationId == '':
                    tableStp = workflow.steps[tableIdMaps[ma.relationId]]
                    stp.rightAttributes[k].relationId = tableStp.model.relation.id



    ctx.context.client.workflowService.update(workflow)




    run_workflow(workflow, project, ctx)
    # END of Worfklow run
    

    # # TODO Create run report regardless of differences in execution result
    # # Get the updated workflow (NOTE: Might be unnecessary)
    workflow = ctx.context.client.workflowService.get(workflow.id)


    #=======================================================================
    # comparison
    # res = jsondiff.diff(refWorkflow.toJson(), workflow.toJson())

    # print(res)
    resultDict = {}
    relTol = workflowInfo["tolerance"]
    # BASIC Workflow info
    # TODO Add this to the report, but not the diff JSON
    # isSuccess = np.all([isinstance(stp.state.taskState, DoneState) for stp in workflow.steps[1:]])
    # isSuccessRef = np.all([isinstance(stp.state.taskState, DoneState) for stp in refWorkflow.steps[1:]])

    
    # # Comparing differences
    # resultDict["Workflow_Name"] = refWorkflow.name

    # if isSuccessRef:
    #     resultDict["Reference_Workflow_Status"] = "SUCCESS"
    # else:
    #     resultDict["Reference_Workflow_Status"] = "FAIL"

    # if isSuccess:
    #     resultDict["Workflow_Status"] = "SUCCESS"
    # else:
    #     resultDict["Workflow_Status"] = "FAIL"

    # taskTime = 0.0
    # taskTimeRef = 0.0

    # for i in range(1, len(workflow.steps)):
    #     # Some steps do not have a taskId (TableStep, DataStep w/o operator)
    #     if len(workflow.steps[i].state.taskId) > 0:
    #         tsk = ctx.context.client.taskService.get(workflow.steps[i].state.taskId)
    #         taskTime += tsk.duration

    #         tsk = ctx.context.client.taskService.get(refWorkflow.steps[i].state.taskId)
    #         taskTimeRef += tsk.duration


    # resultDict["Workflow_Run_Time"] = taskTime
    # resultDict["Reference_Workflow_Run_Time"] = taskTimeRef


    # START step-by-step comparison
    # NOTE Assumes the order of the steps have not changed
    #FIXME Issue #1

    for i in range(0, len(workflow.steps)):
        stp = workflow.steps[i]
        refStp = refWorkflow.steps[i]

        # NOTE TableStep comparison is likely not necessary
        if(isinstance(stp, TableStep)):
            if isinstance(stp.model.relation, SimpleRelation):
                sch = ctx.context.client.tableSchemaService.get(stp.model.relation.id)
                inNames = [c.name for c in sch.columns]
            elif isinstance(stp.model.relation, InMemoryRelation):
                inNames = [c.name for c in stp.model.relation.inMemoryTable.columns]
            else:    
                sch = ctx.context.client.tableSchemaService.get(stp.model.relation.relation.id)
                inNames = [c.name for c in sch.columns]

            if isinstance(refStp.model.relation, SimpleRelation):
                sch = ctx.context.client.tableSchemaService.get(refStp.model.relation.id)
                refInNames = [c.name for c in sch.columns]
            elif isinstance(refStp.model.relation, InMemoryRelation):
                refInNames = [c.name for c in refStp.model.relation.inMemoryTable.columns]
            else:    
                sch = ctx.context.client.tableSchemaService.get(refStp.model.relation.relation.id)
                refInNames = [c.name for c in sch.columns]
            
            # refInNames = [c.name for c in sch.columns]


            res = compare_column_names(inNames, refInNames)

            if len(res) > 0:
                res["Name"] = stp.name
                resultDict["Steps"] = [res]


        if(isinstance(stp, DataStep)):
            # If operator is not set, computedRelation will have no joinOperators
            if hasattr(stp.computedRelation, 'joinOperators'):
                nOutTables = len(stp.computedRelation.joinOperators)
                nOutTablesRef = len(refStp.computedRelation.joinOperators)

                # Step comparison result dictionary
                stpRes = {"Name":stp.name}
                hasDiff = False

                if nOutTables != nOutTablesRef:
                    stpRes["NumTables"] = "Number of output tables do not match: {:d} x {:d} (Reference vs Workflow)".format(
                        nOutTablesRef, nOutTables
                    )

                    #FIXME Issue #2
                else:
                    joinOps = stp.computedRelation.joinOperators
                    refJoinOps = refStp.computedRelation.joinOperators

                    # Table to table comparison
                    for k in range(0, len(joinOps)):
                        tableRes = {}
                        hasDiff = False
                        jop = joinOps[k]
                        refJop = refJoinOps[k]

                        schema = ctx.context.client.tableSchemaService.get(
                            jop.rightRelation.relation.mainRelation.id
                        )

                        refSchema = ctx.context.client.tableSchemaService.get(
                            refJop.rightRelation.relation.mainRelation.id
                        )

                        # Compare schemas
                        refColNames = [c.name for c in refSchema.columns]
                        colNames = [c.name for c in schema.columns]
                        res = compare_column_names(colNames, refColNames)

                        if len(res) > 0:
                            tableRes = {**tableRes, **res}
                            hasDiff = True
                        elif schema.nRows != refSchema.nRows:
                            hasDiff = True
                            tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
                                k + 1,
                                refSchema.nRows,
                                schema.nRows 
                            )
                        else:
                            for ci in range(0, len(colNames)):
                                col = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
                                colVals = col["columns"][0]["values"].astype(float)
                                refCol = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(refSchema.id, [colNames[ci]], 0, -1))
                                refColVals = refCol["columns"][0]["values"].astype(float)

                                rel = np.zeros((len(colVals)))
                                for w in range(0, len(colVals)):
                                    if refColVals[w] == 0 and colVals[w] == 0:
                                        rel[w] = 0
                                    elif (refColVals[w] == 0 and colVals[w] != 0) or (refColVals[w] != 0 and colVals[w] == 0):
                                         rel[w] = 9999
                                    else:
                                        rel[w] = abs(1-colVals[w]/(refColVals[w]))
                                
                                
                                if np.any(rel > relTol):
                                    tableRes["RefValues"] = refColVals
                                    tableRes["Values"] = colVals
                                    tableRes["OutlierIdx"] = which(rel > relTol)
                                    hasDiff = True

                            

                        if hasDiff == True:
                            tableRes = {"TableIdx":k+1}
                            if "Tables" in stpRes.keys():
                                stpRes["Tables"].append(tableRes)
                            else:
                                stpRes["Tables"] = [tableRes]


    ctx.context.client.workflowService.delete(workflow.id, workflow.rev)

    # TODO remove reference and rename new workflow
    # if workflowInfo["updateOnSuccess"] == "True" and len(resultDict) == 0:
    #     print("Updating reference workflow")
    #     refWorkflow = update_operators(refWorkflow, operatorList, ctx)
    #     for stp in refWorkflow.steps[1:]:
    #         stp.state.taskState = InitState()
        

    #     ctx.context.client.workflowService.update(refWorkflow)
    #     run_workflow(refWorkflow, project, ctx)
        

    
    
