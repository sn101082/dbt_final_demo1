

import pandas as pd

from table_data.Table import Table

df = pd.read_excel("pages.xlsx")

#clear extraneous columns

df = df [["schema","Src_Table", "Src_Column", "Target_Table", "Target_col", "Transformation"]]


table = Table.from_data_frame(df)

print(table.generate_model_transform_sql())