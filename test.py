import pandas as pd

df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})

    # Concatenate df2 to df1
df_combined = pd.concat([df1, df2], ignore_index=True)
print(df_combined)