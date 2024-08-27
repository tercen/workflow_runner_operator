import polars as pl
import numpy as np

import workflow_funcs.util as util


from tercen.model.impl import *
import tercen.util.helper_functions as utl
import numpy as np
from tercen.util.helper_objects import ObjectTraverser
import tercen.http.HttpClientService as th

def isnumeric(val):
    return isinstance(val, np.number ) or isinstance(val, int) or isinstance(val, float)


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


def compare_schema(client, tableIdx, schema, refSchema, tol=0, tolType="absolute", hiddenColumns=False, verbose=False):
    tableRes = {}
    hasDiff = False



    # Compare schemas
    refColNamesIn = [c.name for c in refSchema.columns]
    colNamesIn = [c.name for c in schema.columns]
    colNames = []
    refColNames = []
    
    for name in colNamesIn:
        if not name.startswith(".") or hiddenColumns == True:
            colNames.append(name)
    
    
    for name in refColNamesIn:
        if not name.startswith(".") or hiddenColumns == True:
            refColNames.append(name)
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
            util.msg("Comparing {} against {}".format(colNames[ci], refColNames[ci]), verbose=verbose)
            col = th.decodeTSON(client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
            refCol = th.decodeTSON(client.tableSchemaService.selectStream(refSchema.id, [refColNames[ci]], 0, -1))
            colVals = list(col["columns"][0]["values"])
            refColVals = list(refCol["columns"][0]["values"])



            
            if type(colVals[0]) != type(refColVals[0]):
                hasDiff = True
                
                print(hasattr(tableRes, "ColType"))
                if  "ColType" in tableRes:
                    
                    tableRes["ColType"].append( "Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        tableIdx + 1,
                        ci + 1,
                        type(refColVals[0]),
                        type(colVals[0]) 
                    ))
                else:
                    tableRes["ColType"] = ["Column tables do not match for Table {:d}, column {:d} : {} x {} (Reference vs Workflow)".format(
                        tableIdx + 1,
                        ci + 1,
                        type(refColVals[0]),
                        type(colVals[0]) 
                    )]

            
            testEqualityOnly = False
            
            if not isnumeric(col["columns"][0]["values"][0]) or not isnumeric(refColVals[0]):
                testEqualityOnly = True



            if isnumeric(col["columns"][0]["values"]):
                colVals = col["columns"][0]["values"].astype(float)
            if isnumeric(refColVals):
                refColVals = refColVals.astype(float)

            rel = np.zeros((len(colVals)))
            for w in range(0, len(colVals)):
                if testEqualityOnly or tolType.lower() == "equality":
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
                    "OutOfRangeIdx":util.which(rel > tol),
                    "CompResult":list(rel)
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


def compare_export_step(client, tableIdx, stp, refStp,  tol=0, tolType="absolute", tableComp=[], verbose=False,\
                 wkfName=None, refWkfName=None, projectId=None):
    
    expName = "Export-{}-{}".format(wkfName, stp.name)
    gsExpName = "Export-{}-{}".format(refWkfName, refStp.name)

    util.msg("Comparing export tables {} vs {}".format(expName, gsExpName),
              verbose=verbose)

    objs = client.persistentService.getDependentObjects(projectId)
    
    schemas = util.filter_by_type(objs, TableSchema)

    schema = None
    gsSchema = None

    for sch in schemas:
        if sch.name == expName:
            schema = sch

        if sch.name == gsExpName:
            gsSchema = sch

    stpRes = {"Name":stp.name}


    if schema is None:
        stpRes["ExportTable"] = "Result for export step {} not found. \
                            Expecting {}".format( stp.name, expName ) 
        return stpRes
        
    if schema is None:
        stpRes["GSExportTable"] = "Result for export step {} not found for the golden standard. \
                            Expecting {}".format( stp.name, expName ) 
        return stpRes


    res = compare_schema(client, 0, schema, gsSchema,  tol, tolType=tolType)
    tableRes = res[0]
    hasDiff = res[1]

    if hasDiff:
        return {**stpRes, **tableRes}
    else:
        return {}



def compare_step(client, tableIdx, stp, refStp,  tol=0, tolType="absolute", hiddenColumns=False, verbose=False):
    stepResult = {}

    
    if(isinstance(stp, DataStep)):
        # If operator is not set, computedRelation will have no joinOperators and nothing to compare
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

            joinOps = stp.computedRelation.joinOperators
            refJoinOps = refStp.computedRelation.joinOperators

            # Table to table comparison
            k = 0
            while k < len(joinOps) or k < len(refJoinOps):
                
                if k < len(joinOps):
                    jop = joinOps[k]
                else:
                    stpRes["JopNum"] = "JoinOperator {} \
                                is present only in the Golden Standard workflow".format(
                                k+1 )
                    k = k + 1
                    continue

                if k < len(refJoinOps):
                    refJop = refJoinOps[k]
                else:
                    stpRes["JopNum"] = "JoinOperator {} \
                                is present only in the Template workflow".format(
                                k+1 )
                    k = k + 1
                    continue
                # idList = get_simple_relation_id_list(jop.rightRelation)
                # refIdList = get_simple_relation_id_list(refJop.rightRelation)
                traverser = ObjectTraverser()
                relList = traverser.traverse( jop.rightRelation, SimpleRelation )
                idList = [rel.id for rel in relList]
                
                traverser = ObjectTraverser()
                relList = traverser.traverse( refJop.rightRelation, SimpleRelation )
                refIdList = [rel.id for rel in relList]
                if len(idList) != len(refIdList):
                            stpRes["NumTables"] = "Number of Relations in JoinOperator {} \
                                do not match: {:d} x {:d} (GoldenStandard vs Template)".format(
                                k+1, len(refIdList), len(idList) )
                else:
                    for w in range(0, len(idList)):
                        schema = client.tableSchemaService.get(idList[k])
                        refSchema = client.tableSchemaService.get(refIdList[k])
                        
                        res = compare_schema(client, w, schema, refSchema,  tol, tolType=tolType, hiddenColumns=hiddenColumns)
                        tableRes = res[0]
                        hasDiff = res[1]

                if hasDiff == True:
                    tableRes["TableIdx"] = tableIdx+1
                    stepResult = {**stepResult, **tableRes}
                
                k = k +1
    #elif isinstance(stp, ExportStep):


    return stepResult

def diff_workflow(client, workflow, refWorkflow,  tol=0, tolType="absolute", hiddenColumns=False, verbose=False):
    resultDict = []

    if len(workflow.steps) != len(refWorkflow.steps):
        resultDict.append( {"NumOfSteps":"Number of steps between workflow and template are not equal: {} x {}".format(len(workflow.steps),  len(refWorkflow.steps))})

    # NOTE Assume order of steps remains the same
    for i in range(0, len(workflow.steps)):
        if i < len(workflow.steps) and i < len(refWorkflow.steps):
            stp = workflow.steps[i]
            refStp = refWorkflow.steps[i]

            if isinstance(stp.state.taskState, DoneState) and isinstance(refStp.state.taskState, DoneState):
                stepRes = compare_step(client, i, stp, refStp,  tol, tolType, hiddenColumns)

                if len(stepRes) > 0:
                    resultDict.append(stepRes)
            elif isinstance(stp, ExportStep) and isinstance(refStp, ExportStep): 
                stepRes = compare_export_step(client, i, stp, refStp,  tol, tolType,\
                                              wkfName=workflow.name, refWkfName=refWorkflow.name,\
                                              projectId=workflow.projectId, \
                                                  verbose=verbose )
                if len(stepRes) > 0:
                    resultDict.append(stepRes)
            else:
                # A step has not properly run, or has not run yet due to previous failure
                if isinstance(stp.state.taskState, FailedState):
                    stepRes = {"Name":stp.name}
                    stepRes["TaskState"] = "Step did not run successfully"
                    stepRes["FailCode"] = stp.state.taskState.error
                    stepRes["FailReason"] = stp.state.taskState.reason
                    
                    if len(stepRes) > 0:
                        resultDict.append(stepRes)

                if isinstance(stp.state.taskState, InitState):
                    stepRes = {"Name":stp.name}
                    stepRes["TaskState"] = "Step did not run, likely due to a failed preceding step."
                    
                    if len(stepRes) > 0:
                        resultDict.append(stepRes)

        

    return resultDict


    

