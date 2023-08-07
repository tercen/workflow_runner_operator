from datetime import datetime


def msg( message, verbose=False):
    if verbose == True or verbose == "True":
        print("[{}] {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), message))


def which(arr):
    trueIdx = []
    for idx, val in enumerate(arr):
        if val == True:
            trueIdx.append(idx)

    if len(trueIdx) == 1:
        trueIdx = trueIdx[0]
    return trueIdx