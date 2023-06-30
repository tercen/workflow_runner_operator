from tercen.client import context as ctx
import numpy as np
from operator_funcs import calc_mean

tercenCtx = ctx.TercenContext()

df = calc_mean(tercenCtx)
df = tercenCtx.add_namespace(df) 
resDf = tercenCtx.save(df)
