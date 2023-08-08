import sys

sys.path.append('./')
sys.path.append('../../')

from util import msg, which


from tercen.model.base import *
import numpy as np

import tercen.http.HttpClientService as th



def compare_columns_metadata(colNames, refColNames):
    results = {}


    if len(colNames) != len(refColNames):
        results["NumberOfColumns"] = "Different number of column: {:d} vs {:d} (Reference vs New)".format(len(refColNames), len(colNames))

    # CHECK If columns with same name are in the same position
    for i in range(0, len(refColNames)):
        columnFound = False

        for j in range(0, len(colNames)):
            if refColNames[i] == colNames[j]:
                columnFound = True
                if i != j:
                    results["ColumnPosition"] = [ "{} position does not match: {:d} vs {:d} (Reference vs New)".format(refColNames[i], i, j) ]
                break
        
        if not columnFound:
            results["RefColumns"] = [ "Column {} from reference workflow not found.".format(refColNames[i]) ]
               


    for i in range(0, len(colNames)):
        columnFound = False
        for j in range(0, len(refColNames)):
            if colNames[i] == refColNames[j]:
                columnFound = True
                break

        if not columnFound:
            results["Columns"] = [ "Column {} from new workflow not found reference.".format(colNames[i]) ]
                

    return results




def compare_step(ctx, stp, refStp, relTol=0, verbose=False):
    stepResult = {}
    # NOTE Possibly unnecessary, but input data might change
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
        


        res = compare_columns_metadata(inNames, refInNames)
        
        

        if len(res) > 0:
            res["Name"] = stp.name
            stepResult = {**stepResult, **res}
            # resultDict["Steps"] = [res]


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
                    nOutTablesRef, nOutTables )

                #FIXME Issue #2 
                # Produce more meaningful comparison for different number of tables
            else:
                joinOps = stp.computedRelation.joinOperators
                refJoinOps = refStp.computedRelation.joinOperators

                # Table to table comparison
                for k in range(0, len(joinOps)):
                    tableRes = {}
                    hasDiff = False
                    jop = joinOps[k]
                    refJop = refJoinOps[k]

                    if isinstance(jop.rightRelation, SimpleRelation):
                        schema = ctx.context.client.tableSchemaService.get(
                            jop.rightRelation.id
                        )
                        
                    else:
                        schema = ctx.context.client.tableSchemaService.get(
                            jop.rightRelation.relation.mainRelation.id
                        )

                    if isinstance(refJop.rightRelation, SimpleRelation):
                        refSchema = ctx.context.client.tableSchemaService.get(
                            refJop.rightRelation.id
                        )
                    else:
                        refSchema = ctx.context.client.tableSchemaService.get(
                            refJop.rightRelation.relation.mainRelation.id
                        )

                    # Compare schemas
                    refColNames = [c.name for c in refSchema.columns]
                    colNames = [c.name for c in schema.columns]
                    res = compare_columns_metadata(colNames, refColNames)

                    if len(res) > 0:
                        tableRes = {**tableRes, **res}
                        hasDiff = True
                    
                    if schema.nRows != refSchema.nRows:
                        hasDiff = True
                        tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
                            k + 1,
                            refSchema.nRows,
                            schema.nRows 
                        )
                    else:
                        # Same number of columns and same number of rows
                        # We can compare values column-wise
                        
                        
                        for ci in range(0, len(colNames)):
                            msg("Comparing {} against {}".format(colNames[ci], colNames[ci]))
                            col = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
                            colVals = col["columns"][0]["values"]
                            refCol = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(refSchema.id, [refColNames[ci]], 0, -1))
                            refColVals = refCol["columns"][0]["values"]

                            if type(colVals[0]) != type(refColVals[0]):
                                tableRes["ColType"] = "Column tables do not match for Table {:d}, column {:d} : {:d} x {:d} (Reference vs Workflow)".format(
                                    k + 1,
                                    ci + 1,
                                    type(refColVals[0]),
                                    type(colVals[0]) 
                                )

                            def isnumeric(val):
                               return isinstance(val, int) or isinstance(val, float)
                            testEqualityOnly = False
                            if not isnumeric(col["columns"][0]["values"]) or not isnumeric(refCol["columns"][0]["values"]):
                                testEqualityOnly = True


                            if isnumeric(col["columns"][0]["values"]):
                                colVals = col["columns"][0]["values"].astype(float)
                            if isnumeric(refCol["columns"][0]["values"]):
                                refColVals = refCol["columns"][0]["values"].astype(float)

                            rel = np.zeros((len(colVals)))
                            for w in range(0, len(colVals)):
                                if testEqualityOnly:
                                    if refColVals[w] == colVals[w]:
                                        rel[w] = 0
                                    else:
                                        rel[w] = 1
                                else:
                                    if refColVals[w] == 0 and colVals[w] == 0:
                                        rel[w] = 0
                                    elif (refColVals[w] == 0 and colVals[w] != 0) or (refColVals[w] != 0 and colVals[w] == 0):
                                            rel[w] = 9999
                                    else:
                                        rel[w] = abs(1-colVals[w]/(refColVals[w]))
                            
                            
                            if np.any(rel > relTol):
                                colResult = {
                                    "RefColName":refColNames[ci],
                                    "ColName":colNames[ci],
                                    "RefValues":refColVals,
                                    "Values":colVals,
                                    "OutOfRangeIdx":which(rel > relTol),
                                    "CompResult":rel
                                }
                                if hasattr(tableRes, "ColumnResults"):
                                    tableRes["ColumnResults"].append(colResult)
                                else:
                                    tableRes["ColumnResults"] = [colResult]

                                # tableRes["RefValues"] = refColVals
                                # tableRes["Values"] = colVals
                                # tableRes["OutOfRangeIdx"] = which(rel > relTol)
                                # tableRes["CompResult"] = rel
                                hasDiff = True
                                

                        

                    if hasDiff == True:
                        tableRes["TableIdx"]:k+1

                        stepResult = {**stepResult, **tableRes}
    return stepResult

def diff_workflow(ctx, workflow, refWorkflow, relTol=0, verbose=False):
    resultDict = {}
    for i in range(0, len(workflow.steps)):
        stp = workflow.steps[i]
        refStp = refWorkflow.steps[i]

        stepRes = compare_step(ctx, stp, refStp, relTol, verbose)
        resultDict = {**resultDict, **stepRes}
        # NOTE TableStep comparison is likely not necessary
        

    return resultDict


    

    # TODO remove reference and rename new workflow
    # if workflowInfo["updateOnSuccess"] == "True" and len(resultDict) == 0:
    #     print("Updating reference workflow")
    #     refWorkflow = update_operators(refWorkflow, operatorList, ctx)
    #     for stp in refWorkflow.steps[1:]:
    #         stp.state.taskState = InitState()
        

    #     ctx.context.client.workflowService.update(refWorkflow)
    #     run_workflow(refWorkflow, project, ctx)
        

    
    
# if __name__ == '__main__':

    #=======================================================================
    # comparison
    # res = jsondiff.diff(refWorkflow.toJson(), workflow.toJson())

    # print(res)
    
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

