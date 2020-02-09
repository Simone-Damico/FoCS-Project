
# Import
import pandas as pd
import time

# Nel dataframe ci sono 1387432 righe, si usano dei chunk di 100000 righe
df_chunk = pd.read_csv("loans_lenders.csv", chunksize=1000, iterator=True)

chunk_list = []
df_norm = pd.DataFrame(columns=['loan_id', 'lenders'])

t1 = time.time()
n = 0
for chunk in df_chunk:
    chunk = chunk.reset_index()
    print("inizio chunck n {}".format(n))
    for index in range(0, len(chunk)):
        loan_id = chunk['loan_id'][index]
        lender_list = chunk['lenders'][index].split(', ')
        #print("tempo prima della lista {}".format(time.time() - t1))
        #chunk_list = chunk_list + [{'loan_id': loan_id, 'lenders': x} for x in lender_list]
        df_norm = df_norm.append([pd.Series({'loan_id': loan_id, 'lenders': x}) for x in lender_list], ignore_index=True)
        #print("tempo dopo la della lista {}".format(time.time() - t1))
        #print("fatta")
    print("Chunk numero {}, tempo: {}".format(n, time.time() - t1))
    n = n + 1

df_norm = pd.concat(chunk_list)


