import sys
import polars as pl
import numpy as np

from workflow_runner.util import msg, which


from tercen.model.base import *
import numpy as np
import tercen.util.helper_functions as utl
import tercen.http.HttpClientService as th

def isnumeric(val):
    return isinstance(val, int) or isinstance(val, float)


def polarDtype_to_numpyDtype(plType):
    npDtype = None
    if plType == pl.Float64:
        npDtype = np.float64

    if plType == pl.Int64 or plType == pl.Int32:
        npDtype = np.int32

    #if plType == pl.Int32:
    #    npDtype = np.int32

    return npDtype

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

def compare_table(client, tableIdx, jop, refJop, referenceSchemaPath, tol=0, tolType="Absolute"):
    tableRes = {}
    hasDiff = False


    if isinstance(jop.rightRelation, SimpleRelation):
        schema = client.tableSchemaService.get(
            jop.rightRelation.id
        )
        
    else:
        schema = client.tableSchemaService.get(
            jop.rightRelation.relation.mainRelation.id
        )

        
        # if isinstance(refStp.model.relation, SimpleRelation):
        #     #sch = client.tableSchemaService.get(refStp.model.relation.id)
            
        #     tbl = pl.read_csv("{}/{}/data.csv".format(referenceSchemaPath, refStp.model.relation.id) )
        #     refInNames = [c for c in tbl.columns]

        # elif isinstance(refStp.model.relation, InMemoryRelation):
        #     #refInNames = [c.name for c in refStp.model.relation.inMemoryTable.columns]
        #     # TODO Check if this ever occurs in an exported workflow
        #     tbl = pl.read_csv("{}/{}/data.csv", referenceSchemaPath, refStp.model.relation.id)            
        #     refInNames = [c for c in tbl.columns]
        # else:    
        #     #sch = client.tableSchemaService.get(refStp.model.relation.relation.id)
        #     #refInNames = [c.name for c in sch.columns]
        #     tbl = pl.read_csv("{}/{}/data.csv".format(referenceSchemaPath, refStp.model.relation.relation.id) )
        #     refInNames = [c for c in tbl.columns]


    if isinstance(refJop.rightRelation, SimpleRelation):
        refTbl = pl.read_csv("{}/{}/data.csv".format( referenceSchemaPath, refJop.rightRelation.id))
        # refSchema = client.tableSchemaService.get(
        #     refJop.rightRelation.id
        # )
    else:
        refTbl = pl.read_csv("{}/{}/data.csv".format( referenceSchemaPath, refJop.rightRelation.relation.mainRelation.id))
        # refSchema = client.tableSchemaService.get(
        #     refJop.rightRelation.relation.mainRelation.id
        # )

    # Compare schemas
    refColNames = [c for c in refTbl.columns]
    colNames = [c.name for c in schema.columns]
    res = compare_columns_metadata(colNames, refColNames)

    if len(res) > 0:
        tableRes = {**tableRes, **res}
        hasDiff = True

    if schema.nRows != refTbl.shape[0]:
        hasDiff = True
        #TODO Try to get table name here... 
        tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
            tableIdx+1, #k + 1,
            refTbl.shape[0],
            schema.nRows 
        )
    else:
        # Same number of columns and same number of rows
        # We can compare values column-wise
        
        
        for ci in range(0, len(colNames)):
            msg("Comparing {} against {}".format(colNames[ci], colNames[ci]))
            col = th.decodeTSON(client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
            colVals = col["columns"][0]["values"]
            #refCol = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(refSchema.id, [refColNames[ci]], 0, -1))
            #refColVals = refCol["columns"][0]["values"]
            refColVals = refTbl[:,ci]
            refColType = polarDtype_to_numpyDtype(refTbl.dtypes[ci])


            
            if type(colVals[0]) != refColType:
                hasDiff = True
                k = 0 # FIXME Receive the table index for reporting here
                print(hasattr(tableRes, "ColType"))
                if  "ColType" in tableRes:
                    
                    tableRes["ColType"].append( "Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        k + 1,
                        ci + 1,
                        refColType,
                        type(colVals[0]) 
                    ))
                else:
                    tableRes["ColType"] = ["Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        k + 1,
                        ci + 1,
                        refColType,
                        type(colVals[0]) 
                    )]

            
            testEqualityOnly = False
            if not isnumeric(col["columns"][0]["values"]) or not isnumeric(refColVals):
                testEqualityOnly = True


            if isnumeric(col["columns"][0]["values"]):
                colVals = col["columns"][0]["values"].astype(float)
            if isnumeric(refColVals):
                refColVals = refColVals.astype(float)

            rel = np.zeros((len(colVals)))
            for w in range(0, len(colVals)):
                if testEqualityOnly or tolType == "equality":
                    if refColVals[w] == colVals[w]:
                        rel[w] = 0
                    else:
                        rel[w] = 1
                elif tolType == "absolute":
                    rel[w] = abs(refColVals[w]-colVals[w])
                else:

                    if refColVals[w] == 0 and colVals[w] == 0:
                        rel[w] = 0
                    elif (refColVals[w] == 0 and colVals[w] != 0) or (refColVals[w] != 0 and colVals[w] == 0):
                            rel[w] = 9999
                    else:
                        rel[w] = abs(1-colVals[w]/(refColVals[w]))
            
            
            if np.any(rel > tol):
                colResult = {
                    "RefColName":refColNames[ci],
                    "ColName":colNames[ci],
                    "RefValues":refColVals,
                    "Values":colVals,
                    "OutOfRangeIdx":which(rel > tol),
                    "CompResult":rel
                }
                if hasattr(tableRes, "ColumnResults"):
                    tableRes["ColumnResults"].append(colResult)
                else:
                    tableRes["ColumnResults"] = [colResult]

                hasDiff = True
    
    return [tableRes, hasDiff]
    

def __get_colnames(cRel):
    cnames = []

    if isinstance(cRel.mainRelation, CompositeRelation):
        cnames.append(__get_colnames(cRel.mainRelation))
    else:
        if isinstance(cRel.mainRelation, InMemoryRelation):
            cnames.append([c.name for c in  cRel.mainRelation.inMemoryTable.columns])
            cnames = utl.flatten(cnames)

    for jop in cRel.joinOperators:
        if isinstance(jop.rightRelation, InMemoryRelation):
            cnames.append([c.name for c in  jop.rightRelation.inMemoryTable.columns])
            cnames = utl.flatten(cnames)

    cnames = list(set(cnames))
    return cnames


#FIXME Not comparing Gather and Join steps at the moment
def compare_step(client, tableIdx, stp, refStp, referenceSchemaPath, tol=0, tolType="absolute", tableComp=[], verbose=False):
    stepResult = {}
    # NOTE Possibly unnecessary, but input data might change
    # if(isinstance(stp, TableStep)):

    #     if isinstance(stp.model.relation, SimpleRelation):
    #         sch = client.tableSchemaService.get(stp.model.relation.id)
    #         inNames = [c.name for c in sch.columns]
    #     elif isinstance(stp.model.relation, InMemoryRelation):
    #         inNames = [c.name for c in stp.model.relation.inMemoryTable.columns]
    #     elif isinstance(stp.model.relation, CompositeRelation): 
    #         inNames = __get_colnames(stp.model.relation)
    #     else:    
    #         sch = client.tableSchemaService.get(stp.model.relation.relation.id)
    #         inNames = [c.name for c in sch.columns]


        
    #     if isinstance(refStp.model.relation, SimpleRelation):
    #         #sch = client.tableSchemaService.get(refStp.model.relation.id)
            
    #         tbl = pl.read_csv("{}/{}/data.csv".format(referenceSchemaPath, refStp.model.relation.id) )
    #         refInNames = [c for c in tbl.columns]

    #     elif isinstance(refStp.model.relation, InMemoryRelation):
    #         #refInNames = [c.name for c in refStp.model.relation.inMemoryTable.columns]
    #         # TODO Check if this ever occurs in an exported workflow
    #         tbl = pl.read_csv("{}/{}/data.csv", referenceSchemaPath, refStp.model.relation.id)            
    #         refInNames = [c for c in tbl.columns]
    #     else:    
    #         #sch = client.tableSchemaService.get(refStp.model.relation.relation.id)
    #         #refInNames = [c.name for c in sch.columns]
    #         tbl = pl.read_csv("{}/{}/data.csv".format(referenceSchemaPath, refStp.model.relation.relation.id) )
    #         refInNames = [c for c in tbl.columns]

        


    #     res = compare_columns_metadata(inNames, refInNames)


    #     if len(res) > 0:
    #         res["Name"] = stp.name
    #         stepResult = {**stepResult, **res}
    #         # resultDict["Steps"] = [res]


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
                for k in range(0, len(tableComp)):
                    if tableComp["stepId"] == stp.id:
                        tblIdxStrList = tableComp["indexComparison"]
                        pass
            else:
                joinOps = stp.computedRelation.joinOperators
                refJoinOps = refStp.computedRelation.joinOperators

                # Table to table comparison
                for k in range(0, len(joinOps)):
                    jop = joinOps[k]
                    refJop = refJoinOps[k]
                    res = compare_table(client, tableIdx, jop, refJop, referenceSchemaPath, tol)
                    tableRes = res[0]
                    hasDiff = res[1]
                    


                        

                    if hasDiff == True:
                        tableRes["TableIdx"]:tableIdx+1

                        stepResult = {**stepResult, **tableRes}
    return stepResult

def diff_workflow(client, workflow, refWorkflow, referenceSchemaPath, tol=0, tolType="absolute", verbose=False):
    resultDict = []

    if len(workflow.steps) != len(refWorkflow.steps):
        resultDict.append( {"NumOfSteps":"Number of steps between workflow and template are not equal: {} x {}".format(len(workflow.steps),  len(refWorkflow.steps))})

    for i in range(0, len(workflow.steps)):
        if i < len(workflow.steps) and i < len(refWorkflow.steps):
            stp = workflow.steps[i]
            refStp = refWorkflow.steps[i]

            #TODO
            # Compare if fully ran or failed step
            # UPLOAD current gs and workflow
            #if stp.state
            if isinstance(stp.state.taskState, DoneState) and isinstance(refStp.state.taskState, DoneState):
                stepRes = compare_step(client, i, stp, refStp, referenceSchemaPath, tol, tolType, verbose)
                # resultDict = {**resultDict, **stepRes}
                if len(stepRes) > 0:
                    resultDict.append(stepRes)
            else:
                if isinstance(stp.state.taskState, FailedState):
                    stepRes = {"Name":stp.name}
                    stepRes["TaskState"] = "Step did not run successfully"
                    # resultDict = {**resultDict, **stepRes}
                    if len(stepRes) > 0:
                        resultDict.append(stepRes)

                if isinstance(stp.state.taskState, InitState):
                    stepRes = {"Name":stp.name}
                    stepRes["TaskState"] = "Step did not run, likely due to a failed preceding step."
                    # resultDict = {**resultDict, **stepRes}
                    if len(stepRes) > 0:
                        resultDict.append(stepRes)

        

    return resultDict


    

