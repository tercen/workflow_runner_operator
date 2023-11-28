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

def compare_table(ctx,jop, refJop, tol=0, tolType="Absolute"):
    tableRes = {}
    hasDiff = False
    # jop = joinOps[k]
    # refJop = refJoinOps[k]

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
        #TODO Try to get table name here... 
        tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
            1, #k + 1,
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
    

#FIXME Not comparing Gather and Join steps at the moment
def compare_step(ctx, stp, refStp, tol=0, tolType="absolute", tableComp=[], verbose=False):
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
                    # res = compare_table(ctx,jop, refJop, tol)
                    # tableRes = res[0]
                    # hasDiff = res[1]
                    
                    tableRes = {}
                    hasDiff = False
                    # jop = joinOps[k]
                    # refJop = refJoinOps[k]

                    # if isinstance(jop.rightRelation, SimpleRelation):
                    #     schema = ctx.context.client.tableSchemaService.get(
                    #         jop.rightRelation.id
                    #     )
                        
                    # else:
                    #     schema = ctx.context.client.tableSchemaService.get(
                    #         jop.rightRelation.relation.mainRelation.id
                    #     )

                    # if isinstance(refJop.rightRelation, SimpleRelation):
                    #     refSchema = ctx.context.client.tableSchemaService.get(
                    #         refJop.rightRelation.id
                    #     )
                    # else:
                    #     refSchema = ctx.context.client.tableSchemaService.get(
                    #         refJop.rightRelation.relation.mainRelation.id
                    #     )

                    # # Compare schemas
                    # refColNames = [c.name for c in refSchema.columns]
                    # colNames = [c.name for c in schema.columns]
                    # res = compare_columns_metadata(colNames, refColNames)

                    # if len(res) > 0:
                    #     tableRes = {**tableRes, **res}
                    #     hasDiff = True
                    
                    # if schema.nRows != refSchema.nRows:
                    #     hasDiff = True
                    #     tableRes["NumRows"] = "Number rows tables do not match for Table {:d} : {:d} x {:d} (Reference vs Workflow)".format(
                    #         k + 1,
                    #         refSchema.nRows,
                    #         schema.nRows 
                    #     )
                    # else:
                    #     # Same number of columns and same number of rows
                    #     # We can compare values column-wise
                        
                        
                    #     for ci in range(0, len(colNames)):
                    #         msg("Comparing {} against {}".format(colNames[ci], colNames[ci]))
                    #         col = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(schema.id, [colNames[ci]], 0, -1))
                    #         colVals = col["columns"][0]["values"]
                    #         refCol = th.decodeTSON(ctx.context.client.tableSchemaService.selectStream(refSchema.id, [refColNames[ci]], 0, -1))
                    #         refColVals = refCol["columns"][0]["values"]

                    #         if type(colVals[0]) != type(refColVals[0]):
                    #             tableRes["ColType"] = "Column tables do not match for Table {:d}, column {:d} : {:d} x {:d} (Reference vs Workflow)".format(
                    #                 k + 1,
                    #                 ci + 1,
                    #                 type(refColVals[0]),
                    #                 type(colVals[0]) 
                    #             )

                    #         def isnumeric(val):
                    #            return isinstance(val, int) or isinstance(val, float)
                    #         testEqualityOnly = False
                    #         if not isnumeric(col["columns"][0]["values"]) or not isnumeric(refCol["columns"][0]["values"]):
                    #             testEqualityOnly = True


                    #         if isnumeric(col["columns"][0]["values"]):
                    #             colVals = col["columns"][0]["values"].astype(float)
                    #         if isnumeric(refCol["columns"][0]["values"]):
                    #             refColVals = refCol["columns"][0]["values"].astype(float)

                    #         rel = np.zeros((len(colVals)))
                    #         for w in range(0, len(colVals)):
                    #             if testEqualityOnly or tolType == "equality":
                    #                 if refColVals[w] == colVals[w]:
                    #                     rel[w] = 0
                    #                 else:
                    #                     rel[w] = 1
                    #             elif tolType == "absolute":
                    #                 rel[w] = abs(refColVals[w]-colVals[w])
                    #             else:

                    #                 if refColVals[w] == 0 and colVals[w] == 0:
                    #                     rel[w] = 0
                    #                 elif (refColVals[w] == 0 and colVals[w] != 0) or (refColVals[w] != 0 and colVals[w] == 0):
                    #                         rel[w] = 9999
                    #                 else:
                    #                     rel[w] = abs(1-colVals[w]/(refColVals[w]))
                            
                            
                    #         if np.any(rel > tol):
                    #             colResult = {
                    #                 "RefColName":refColNames[ci],
                    #                 "ColName":colNames[ci],
                    #                 "RefValues":refColVals,
                    #                 "Values":colVals,
                    #                 "OutOfRangeIdx":which(rel > tol),
                    #                 "CompResult":rel
                    #             }
                    #             if hasattr(tableRes, "ColumnResults"):
                    #                 tableRes["ColumnResults"].append(colResult)
                    #             else:
                    #                 tableRes["ColumnResults"] = [colResult]

                    #             hasDiff = True
                                

                        

                    if hasDiff == True:
                        tableRes["TableIdx"]:k+1

                        stepResult = {**stepResult, **tableRes}
    return stepResult

def diff_workflow(ctx, workflow, refWorkflow, tol=0, tolType="absolute", verbose=False):
    resultDict = {}
    for i in range(0, len(workflow.steps)):
        stp = workflow.steps[i]
        refStp = refWorkflow.steps[i]

        stepRes = compare_step(ctx, stp, refStp, tol, tolType, verbose)
        resultDict = {**resultDict, **stepRes}
        # NOTE TableStep comparison is likely not necessary
        

    return resultDict


    

