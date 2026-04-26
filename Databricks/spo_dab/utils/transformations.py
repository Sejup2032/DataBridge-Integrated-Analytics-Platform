# In transformations.py
class reusable:
    def __init__(self):
        pass

    def dropColumns(self, df, column_list):
        # This takes the dataframe and drops the specified columns
        return df.drop(*column_list)