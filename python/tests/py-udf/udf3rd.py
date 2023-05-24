import pickle
import pandas as pd

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0.0)

def finish(buf):
    return pickle.loads(buf)

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    row = []
    for i in range(rows):
        for j in range(cols):
            row.append(datablock.data(i, j))
    df = pd.DataFrame(row)
    new_state = df.cumsum().iloc[-1][0] + state
    return pickle.dumps(new_state)
