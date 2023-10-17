import os , io
import sys
import json

import copy, string, random, tempfile, subprocess



from datetime import datetime

from workflow_runner.util import msg, which


from tercen.model.base import *
from tercen.model.base import FileDocument,  InitState,  ImportGitDatasetTask, RunComputationTask
from tercen.client import context as tercen

import numpy as np

from datetime import datetime

import tercen.util.helper_functions as utl
from pytson import encodeTSON

import polars as pl



def get_installed_operator(client, installedOperators, opName, opUrl, opVersion, verbose=False):
    opTag = '{}@{}@{}'.format(opName, opUrl, opVersion)
    comp = [opTag ==  '{}@{}@{}'.format(iop.name, iop.url.uri, iop.version) for iop in installedOperators]


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
        idx = which(comp)
        if isinstance(idx, list):
            idx = idx[0]
        operator = installedOperators[idx]


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
            opName = stp.model.operatorSettings.operatorRef.name
            opUrl = stp.model.operatorSettings.operatorRef.url.uri
            opVersion = stp.model.operatorSettings.operatorRef.version

            #opTag = '{}@{}'.format(opUrl, opVersion)
            #comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

            # FIXME DEBUG from here
            if opUrl != '':
                operator = get_installed_operator(client, installedOperators, opName, opUrl, opVersion)



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


def __get_file_id(client, user, tbf, projectId):

    if "fileId" in tbf:
        return tbf["fileId"]
    else:
        # 1: Search for the filename in the dataset library
        dsLib = client.documentService.getTercenDatasetLibrary(0, 100)

        fileDoc = None
        for l in dsLib:
            if l.name == tbf["filename"]:
                fileDoc = l
                break

        if not fileDoc == None:
            gitTask = ImportGitDatasetTask()
            gitTask.state = InitState()
            gitTask.gitToken = os.environ["GITHUB_TOKEN"]
            gitTask.projectId = projectId
            gitTask.url = fileDoc.url
            gitTask.version = fileDoc.version
            gitTask.owner = user

            gitTask = client.taskService.create( gitTask )
            client.taskService.runTask(gitTask.id)
            gitTask = client.taskService.waitDone(gitTask.id)

            return gitTask.schemaId
        else:
            docs = client.projectDocumentService.findSchemaByOwnerAndLastModifiedDate(user, "")
            
            fname = tbf["filename"].split("/")[-1]

            docComp = [doc.name == fname and doc.projectId == projectId for doc in docs]
            if len(docs) == 0 or not np.any(docComp):
                
                tercenDocs = client.documentService.getTercenDatasetLibrary(0,100)
                docComp = [doc.name == tbf["filename"] for doc in docs]

                if len(tercenDocs) == 0 or not np.any(docComp):
                    raise FileNotFoundError("!!ERROR!! Document {} not found. Cannot set TableStep, so aborting execution.".format(tbf["filename"]))
                else:
                    # upload it to the project
                    #doc = tercenDocs[which(docComp)[0]]
                    doc = tercenDocs[0]

                    
                    file = FileDocument()
                    file.name = doc.name
                    file.acl.owner = user
                    file.projectId = projectId
                    # bytes_data = encodeTSON(doc.toJson()).getvalue()
                    file = client.fileService.uploadTable(file, doc.toJson())
                    client.d

                    pass
            idx = which(docComp)

            # TODO Abort if filename does not exist
            if isinstance(idx, list):
                doc = docs[idx[0]]
            else:
                doc = docs[idx]

            return doc.id



def __upload_file_as_table(client, filename, projectId, user, cellranger=True):
        
        #FIXME Will likely be removed later
        if cellranger == True:
            opList =client.documentService.getTercenOperatorLibrary(0,1)

            crOpIdx = which(  [op.name == "Cell Ranger" for op in opList]  )
            cellrangerOp = opList[crOpIdx]

            with open(filename, 'rb') as file_data:
                bytesData = file_data.read()

            file = FileDocument()
            file.name = filename.split("/")[-1]
            file.acl.owner = user
            file.projectId = projectId
            
            file = client.fileService.upload(file, bytesData)


            pFile = FileDocument()
            pFile.name = "documentIdFile"
            pFile.acl.owner = user
            pFile.projectId = projectId
            
            df = pl.DataFrame({"documentId":file.id})
            pFile = client.fileService.uploadTable(pFile, utl.dataframe_to_table(df)[0].toJson())


            query = CubeQuery()
            col = Factor()
            col.name = "documentId"
            col.type = "string"
            query.colColumns = [col]
            rl = InMemoryRelation()
            rl.inMemoryTable = utl.dataframe_to_table(pl.DataFrame({"documentId":[file.id]}), values_as_list=True)[0]
            query.relation = rl
            query.axisQueries = [CubeAxisQuery()]


            installedOps = client.documentService.findOperatorByOwnerLastModifiedDate(user,"")
            idx = which([o.name == cellrangerOp.name for o in installedOps])
            if idx != []:
                cellrangerOp = installedOps[idx] 

            # ops = client.operatorService
            opSettings = OperatorSettings()
            opSettings.namespace = "ns001"

            opRef = OperatorRef()
            opRef.operatorId = cellrangerOp.id
            opRef.operatorKind = str(cellrangerOp).split(".")[-1].split(" ")[0]
            opRef.name = cellrangerOp.name
            opRef.version = cellrangerOp.version
            opSettings.operatorRef = opRef
            query.operatorSettings = opSettings

            

            rcTask = RunComputationTask()
            rcTask.state = InitState()
            rcTask.query = query
            rcTask.owner = user
            rcTask.projectId = projectId
            #rcTask.fileResultId = file.id

            rcTask = client.taskService.create( rcTask )
            client.taskService.runTask(rcTask.id)
            rcTask = client.taskService.waitDone(rcTask.id)

            #TODO Clean up
            if isinstance(rcTask.computedRelation, CompositeRelation):
                tableSchemas = __get_table_schemas(rcTask.computedRelation.joinOperators[0], client)
                
            else:
                tableSchemas = __get_table_schemas(rcTask.computedRelation, client)
            
            tableSchemas = utl.flatten(tableSchemas)
            for ts in tableSchemas:
                for col in ts.columns:
                    if not str.startswith(col.name, "."):
                        col.name = col.name.split(".")[-1]
                        col.id = col.id.split(".")[-1]



            return tableSchemas

            
            
        else:
            df = pl.read_csv(filename)
            df = df.with_columns(pl.col(pl.INTEGER_DTYPES).cast(pl.Int32))
            table = utl.dataframe_to_table(df)[0]

            file = FileDocument()
            file.name = filename.split("/")[-1]
            file.acl.owner = user
            file.projectId = projectId
            # bytes_data = "hello\n\nhello\n\n42".encode("utf_8")
            # file = self.client.fileService.upload(file, bytes_data)
            file = client.fileService.uploadTable(file, table.toJson())

            task = CSVTask()
            task.state = InitState()
            task.fileDocumentId = file.id
            task.projectId = projectId
            task.owner = user

            task = client.taskService.create( task )
            client.taskService.runTask(task.id)
            csvTask = client.taskService.waitDone(task.id)

            return None

def __get_table_schemas(joinOp, client):
    tss = client.tableSchemaService
    tableSchemas = []
    
    if hasattr(joinOp , "rightRelation") and hasattr(joinOp.rightRelation , "mainRelation"):
        msch = tss.get(joinOp.rightRelation.mainRelation.id)
        cNames = [c.name for c in msch.columns]
        dt = client.tableSchemaService.select(msch.id, cNames,0, msch.nRows)
        tableSchemas.append( dt)

    if hasattr(joinOp , "rightRelation") and isinstance(joinOp.rightRelation , SimpleRelation):
        msch = tss.get(joinOp.rightRelation.id)
        cNames = [c.name for c in msch.columns]
        dt = client.tableSchemaService.select(msch.id, cNames,0, msch.nRows)
        tableSchemas.append(dt)

    if hasattr(joinOp , "rightRelation") and isinstance(joinOp.rightRelation , CompositeRelation):
        for i in range(0, len(joinOp.rightRelation.joinOperators)):
            tableSchemas.append(__get_table_schemas(joinOp.rightRelation.joinOperators[i], client))
    
    return tableSchemas

# Separate function for legibility
def update_table_relations(client, refWorkflow, workflow, filemap, user, verbose=False):
    msg("Setting up table step references in new workflow.", verbose)
    if refWorkflow == None:
        refWorkflow = client.workflowService.get(workflowInfo["workflowId"])

    
    if filemap == None:
        # No filename of tableStep <-> filename association has been given
        # Trying to derive it from the table step name
        for i in range(0, len(refWorkflow.steps)):
            if isinstance(refWorkflow.steps[i], TableStep):
                stp = refWorkflow.steps[i]
                filename = {"filename":stp.name} # This is not necessarily so
                fileId = __get_file_id(client, user, filename, workflow.projectId)
                rr = __file_relation(client, fileId)
                workflow.steps[i].model.relation = rr
                workflow.steps[i].state.taskState = DoneState()

    elif isinstance(filemap, str):

        if filemap.startswith("repo:"):
            # Download as local file first
            filemap = filemap.split("repo:")[-1]
            urlParts = filemap.split("@")

            
            gitCmd = 'https://github.com/{}/raw/main/{}'.format(urlParts[0], urlParts[1])
            tmpDir = "{}/AA_{}".format(tempfile.gettempdir(), ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)))

            os.makedirs(tmpDir)

            zipFileName = "{}/{}".format(tmpDir, urlParts[1].split("/")[-1])
            subprocess.call(['wget', '-O', zipFileName, gitCmd])



            #subprocess.call(["unzip", '-qq', '-d', tmpDir, '-o', zipFileName])

            #res = __upload_file_as_table(client, filemap, workflow.projectId, user)
            #TODO CHECK FILENAME
            filemap = "file:{}".format(zipFileName)

        if filemap.startswith("file:"):
            #local file
            filemap = filemap.split("file:")[-1]
            res = __upload_file_as_table(client, filemap, workflow.projectId, user)
            filemap = res

        filemap = {"filename":filemap}
        for i in range(0, len(workflow.steps)):
            
            if isinstance(workflow.steps[i], TableStep):
                #print( tableStepFiles[0])
                if isinstance(filemap["filename"], str):
                    fileId = __get_file_id(client, user, filemap, workflow.projectId)
                    rr = __file_relation(client, fileId)
                    workflow.steps[i].model.relation = rr
                    workflow.steps[i].state.taskState = DoneState()
                else:
                    # List of table schemas
                    rel = CompositeRelation()
                    nTables = len(filemap["filename"])

                    # "Main" Table --> Table 1
                    jop = utl.as_join_operator(filemap["filename"][0], [], [])
                    rel.joinOperators = [jop]

                    #FIXME
                    # Currently, only 3 tables handles
                    # If this code remains, then this should be adjusted
                    if nTables > 1:
                        # Col & Row Tables
                        rel.mainRelation = utl.as_composite_relation(filemap["filename"][0])
                        

                        cNames = set([c.name for c in filemap["filename"][0].columns]  )
                        jops = []
                        for k in range(1, nTables):
                            cNames2 = set([c.name for c in filemap["filename"][k].columns]  )
                            joinNames = cNames.intersection(cNames2)
                            jops.append(utl.as_join_operator(filemap["filename"][k], list(joinNames), list(joinNames)))

                        rel.mainRelation.joinOperators = jops
                    else:
                        rel.mainRelation = utl.as_relation(filemap["filename"][0])
        
                    #wkf = client.workflowService.get(workflow.id)
                    workflow.steps[i].model.relation = rel # ur
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

    