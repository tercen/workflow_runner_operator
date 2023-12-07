import os 

import copy, string, random, tempfile, subprocess



from datetime import datetime

from util import msg, which


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

    if opTag == '@@' or opUrl == '':
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

def update_operators( workflow, client, params,  verbose=False):
    installedOperators = client.documentService.findOperatorByOwnerLastModifiedDate(params['user'], '')
    operatorLib = client.documentService.getTercenOperatorLibrary(0, 300)


    for stpIdx in range(0, len(workflow.steps)):
        if hasattr(workflow.steps[stpIdx].model, "operatorSettings"):
            opName = workflow.steps[stpIdx].model.operatorSettings.operatorRef.name
            opVersion = workflow.steps[stpIdx].model.operatorSettings.operatorRef.version


            opIdx = which([op.name == opName for op in operatorLib])

            if opIdx != []:
                libOp = operatorLib[opIdx]
                if libOp.version > opVersion:
                    msg("Updating {} operator from version {} to version {}".format(\
                        opName, libOp.version, opVersion), verbose)
                    
                    operator = get_installed_operator(client, installedOperators, \
                                                      libOp.name, \
                                                      libOp.url.uri, libOp.version, params)
                    
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.operatorId = operator.id
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.name = operator.name
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.url = operator.url
                    workflow.steps[stpIdx].model.operatorSettings.operatorRef.version = operator.version
    
    return workflow


def setup_workflow(client, templateWkf, gsWkf, params, update_operator_version=False, verbose=False):
    # Copy is wanted in the case of multiple golden standards being tested
    msg("Copying workflow", verbose)
    workflow = copy.deepcopy(templateWkf)

    workflow.name = "{}_{}".format(templateWkf.name, datetime.now().strftime("%Y%m%d_%H%M%S"))
    workflow.id = ''

    if update_operator_version == True:
        msg("Checking for updated operator versions", verbose)
        workflow = update_operators( workflow, client, params, verbose)
    
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

    workflow = client.workflowService.create(workflow)

    try:
        update_table_relations(client, workflow, gsWkf, verbose=verbose )
    except FileNotFoundError as e:
        print(e)
        workflow.steps = []
        client.workflowService.delete(workflow.id, workflow.rev)



    return workflow

# Separate function for legibility
def update_table_relations(client, workflow, gsWorkflow, verbose=False):
    msg("Setting up table step references in new workflow.", verbose)

    for gsStp in gsWorkflow.steps:
        if isinstance(gsStp, TableStep):
            # Number of steps might have changed
            for i in range(0, len(workflow.steps)):
                stp = workflow.steps[i]
                # NOTE Assumes the golden standard was a clone of an execution of the template
                # This assumption MIGHT NOT hold
                if stp.id == gsStp.id:
                    stp.model = copy.deepcopy(gsStp.model)
                    stp.state.taskState = DoneState()
                    


    client.workflowService.update(workflow)

    