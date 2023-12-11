import polars as pl
import numpy as np

from .util import msg, which


from tercen.model.impl import *
import tercen.util.helper_functions as utl
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


def get_simple_relation_id_list(obj):
    idList = []

    if isinstance(obj, SimpleRelation):
        idList.append(obj.id)
    elif isinstance(obj, CompositeRelation):
        idList.append(get_simple_relation_id_list(obj.mainRelation))
        idList.append(get_simple_relation_id_list(obj.joinOperators))

    elif isinstance(obj, RenameRelation):
        cRel = obj.relation
        idList.append(get_simple_relation_id_list(cRel.mainRelation))
        idList.append(get_simple_relation_id_list(cRel.joinOperators))
    elif isinstance(obj, list):
        # Assumed: List of JoinOperator!
        for o in obj:
            idList.append(get_simple_relation_id_list(o.rightRelation))

      

        
    
    idList = utl.flatten(idList)
    return idList


def compare_schema(client, tableIdx, schema, refSchema, tol=0, tolType="Absolute"):
    tableRes = {}
    hasDiff = False



    # Compare schemas
    refColNames = [c.name for c in refSchema.columns]
    colNames = [c.name for c in schema.columns]
    res = compare_columns_metadata(colNames, refColNames)

    if len(res) > 0:
        tableRes = {**tableRes, **res}
        hasDiff = True

    if schema.nRows != refSchema.nRows:
        hasDiff = True
        #TODO Try to get table name here... 
        tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
            tableIdx+1, #k + 1,
            refSchema.nRows,
            schema.nRows 
        )
    else:
        # Same number of columns and same number of rows
        # We can compare values column-wise
        
        
        for ci in range(0, len(colNames)):
            msg("Comparing {} against {}".format(colNames[ci], refColNames[ci]))
            col = th.decodeTSON(client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
            refCol = th.decodeTSON(client.tableSchemaService.selectStream(refSchema.id, [refColNames[ci]], 0, -1))
            colVals = col["columns"][0]["values"]
            refColVals = refCol["columns"][0]["values"]



            
            if type(colVals[0]) != type(refColVals[0]):
                hasDiff = True
                
                print(hasattr(tableRes, "ColType"))
                if  "ColType" in tableRes:
                    
                    tableRes["ColType"].append( "Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        tableIdx + 1,
                        ci + 1,
                        refColType,
                        type(colVals[0]) 
                    ))
                else:
                    tableRes["ColType"] = ["Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        tableIdx + 1,
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
def compare_step(client, tableIdx, stp, refStp,  tol=0, tolType="absolute", tableComp=[], verbose=False):
    stepResult = {}


    if(isinstance(stp, DataStep)):
        # If operator is not set, computedRelation will have no joinOperators
        if hasattr(stp.computedRelation, 'joinOperators'):
            # "High-level" output tables
            # Each join op might contain multiple related tables (e.g. .ri and .ci tables)
            nOutJop = len(stp.computedRelation.joinOperators)
            nOutJopRef = len(refStp.computedRelation.joinOperators)

            # Step comparison result dictionary
            stpRes = {"Name":stp.name}
            hasDiff = False

            if nOutJop != nOutJopRef:
                stpRes["NumJop"] = "Number of JoinOperators do not match: {:d} x {:d} (Reference vs Template)".format(
                    nOutJopRef, nOutJop )

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

                    idList = get_simple_relation_id_list(jop.rightRelation)
                    refIdList = get_simple_relation_id_list(refJop.rightRelation)

                    if len(idList) != len(refIdList):
                                stpRes["NumTables"] = "Number of Tables in JoinOperator {} \
                                    do not match: {:d} x {:d} (GoldenStandard vs Template)".format(
                                    k+1, len(refIdList), len(idList) )
                    else:
                        for w in range(0, len(idList)):
                            schema = client.tableSchemaService.get(idList[5])
                            refSchema = client.tableSchemaService.get(idList[5])
                            res = compare_schema(client, w, schema, refSchema,  tol)
                            tableRes = res[0]
                            hasDiff = res[1]

                    if hasDiff == True:
                        tableRes["TableIdx"]:tableIdx+1
                        stepResult = {**stepResult, **tableRes}
    return stepResult

def diff_workflow(client, workflow, refWorkflow,  tol=0, tolType="absolute", verbose=False):
    resultDict = []

    if len(workflow.steps) != len(refWorkflow.steps):
        resultDict.append( {"NumOfSteps":"Number of steps between workflow and template are not equal: {} x {}".format(len(workflow.steps),  len(refWorkflow.steps))})

    # NOTE Assume order of steps remains the same
    for i in range(0, len(workflow.steps)):
        if i < len(workflow.steps) and i < len(refWorkflow.steps):
            stp = workflow.steps[i]
            refStp = refWorkflow.steps[i]

            if isinstance(stp.state.taskState, DoneState) and isinstance(refStp.state.taskState, DoneState):
                stepRes = compare_step(client, i, stp, refStp,  tol, tolType, verbose)

                if len(stepRes) > 0:
                    resultDict.append(stepRes)
            else:
                # A step has not properly run, or has not run yet due to previous failure
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


    

