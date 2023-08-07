import sys

sys.path.append('./')
sys.path.append('../../')

from util import msg, which


from tercen.model.base import *
import numpy as np

import tercen.http.HttpClientService as th



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


def diff_workflow(ctx, workflow, refWorkflow, relTol=0):
    resultDict = []
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

                            resultDict.append(stpRes)

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

