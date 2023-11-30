import os 

import copy, string, random, tempfile, subprocess



from datetime import datetime

from workflow_runner.util import msg, which


from tercen.model.impl import *
from tercen.client import context as tercen

import numpy as np

from datetime import datetime

import tercen.util.helper_functions as utl
from pytson import encodeTSON

import polars as pl



def get_installed_operator(client, installedOperators, opName, opUrl, opVersion, params, verbose=False):
    opTag = '{}@{}@{}'.format(opName, opUrl, opVersion)
    comp = [opTag ==  '{}@{}@{}'.format(iop.name, iop.url.uri, iop.version) for iop in installedOperators]

    if opTag == '':
        return None
    
    if not np.any(comp):
        # install the operator
        msg("Installing {}:{} from {}".format(opName, opVersion, opUrl), verbose)
        installTask = CreateGitOperatorTask()
        installTask.state = InitState()
        installTask.url.uri = opUrl
        installTask.version = opVersion
        installTask.gitToken = params["gitToken"]
        
        installTask.testRequired = True
        installTask.isDeleted = False
        installTask.owner = params["user"]

        installTask = client.taskService.create(installTask)
        client.taskService.runTask(installTask.id)
        installTask = client.taskService.waitDone(installTask.id)

        if isinstance(installTask.state, DoneState):
            operator = client.operatorService.get(installTask.operatorId)
        else:
            raise Exception("Operator " + opTag + " failed. USER: " + params["user"]) 
    else:
        idx = which(comp)
        if isinstance(idx, list):
            idx = idx[0]

        operator = installedOperators[idx]

        # print("Adding {}:{}".format(operator.name, operator.version))


    return operator
    
def __is_operator_installed(opName, opUrl, opVersion, installedOperators):
    opTag = '{}@{}@{}'.format(opName, opUrl, opVersion)
    comp = [opTag ==  '{}@{}@{}'.format(iop.name, iop.url.uri, iop.version) for iop in installedOperators]

def update_operators(workflow, refWorkflow, operatorList, client, params, verbose=False):
    installedOperators = client.documentService.findOperatorByOwnerLastModifiedDate(params['user'], '')


    # NOTE
    # Operator Id from reference workflow might be different than the current workflow instance
    # E.g.: Reference workflow is from remote instance while the test is being running locally
    # 
    # The first step, then, is to add the correct, installed operator id's to the workflow to be run
    for stpIdx in range(0, len(workflow.steps)):
        stp = workflow.steps[stpIdx]
        

        if stp.__class__ == DataStep:
            opName = stp.model.operatorSettings.operatorRef.name
            opUrl = stp.model.operatorSettings.operatorRef.url.uri
            opVersion = stp.model.operatorSettings.operatorRef.version

            operator = get_installed_operator(client, installedOperators, opName, opUrl, opVersion, params)


            if operator != None:
                workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
                workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
                workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
                workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version



    # Gets the required operators for the update (install them if necessary)
    for op in operatorList:
        #opTag = '{}@{}'.format(op["operatorURL"], op["version"])
        #comp = [opTag ==  '{}@{}'.format(iop.url.uri, iop.version) for iop in installedOperators]

        operator = get_installed_operator(client, installedOperators, op["name"], op["operatorURL"], op["version"], params)

        if operator != None:
            stpIdx = which([op["stepId"] == stp.id for stp in refWorkflow.steps])
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
            workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow


def create_test_workflow(client, templateWkf, params, verbose=False):
    workflow = copy.deepcopy(templateWkf)

    # READ list of operators from input json and update accordingly in the cloned workflow
    if hasattr(params, "operators"):
        operatorList = params["operators"]
    else:
        operatorList = []

    msg("Copying workflow", verbose)
    
    workflow.name = "{}_{}".format(templateWkf.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    workflow = update_operators(workflow, templateWkf, operatorList, client, params)
    
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

    return [workflow,templateWkf]

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


def __get_file_id(client, user, tbf, projectId, gitToken):

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
            gitTask.gitToken = gitToken #os.environ["GITHUB_TOKEN"]
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


                    pass
            idx = which(docComp)

            # TODO Abort if filename does not exist
            if isinstance(idx, list):
                doc = docs[idx[0]]
            else:
                doc = docs[idx]

            return doc.id



# def __upload_file_as_table(client, filename, projectId, user, cellranger=False, params):
        
        #FIXME Will likely be removed later
        if cellranger == True:
            opList =client.documentService.getTercenOperatorLibrary(0,1)
            installedOps = client.documentService.findOperatorByOwnerLastModifiedDate(user,"")

            crOpIdx = which(  [op.name == "Cell Ranger" for op in opList]  )
            cellrangerOp = opList[crOpIdx]
            cellrangerOp = get_installed_operator(client, installedOps, cellrangerOp.name, cellrangerOp.url.uri, cellrangerOp.version, params)

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
def update_table_relations(client, workflow, gsWorkflow, inputFileList, user, gitToken, verbose=False, cellranger=False):
    msg("Setting up table step references in new workflow.", verbose)

    for gsStp in gsWorkflow.steps:
        if isinstance(gsStp, TableStep):
            # Number of steps might have changed
            for i in range(0, len(workflow.steps)):
                stp = workflow.steps[i]
                if stp.id == gsStp.id:
                    # rr = __file_relation(client, inputFileList[0].id)
           
                    stp.model = gsStp.model
                    stp.state.taskState = DoneState()
                    


    client.workflowService.update(workflow)

    