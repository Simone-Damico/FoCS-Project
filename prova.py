from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

import pandas as pd
import numpy as np
import time

# Create a dataframe to be processed
df_chunk = pd.read_csv("loans_lenders.csv", chunksize=10000)

df_norm = pd.DataFrame(columns=['loan_id', 'lenders'])

# Apply to dataframe
def apply_to_df(df_chunks):
    df_chunks = df_chunks.reset_index()
    res = pd.DataFrame(columns=['loan_id', 'lenders'])
    for index in range(0, len(df_chunks)):
        loan_id = df_chunks['loan_id'][index]
        lender_list = df_chunks['lenders'][index].split(', ')
        res = res.append([{'loan_id': loan_id, 'lenders': x} for x in lender_list], ignore_index=True)
    return res


# Divide dataframe to chunks
prs = 100 # define the number of processes
t1 = time.time()
n = 1
for chunk in df_chunk:
    print("inizio chunck n {}".format(n))
    chunk_partition = np.array_split(chunk, int(chunk.shape[0]/prs))

    # Process dataframes
    with ThreadPool(prs) as p:
        result = p.map(apply_to_df, chunk_partition)

    print("Chunk numero {}, tempo: {}".format(n, time.time() - t1))

# Concat all chunks
    df_norm = df_norm.append(result, ignore_index=True)
    n += 1

print("finito")