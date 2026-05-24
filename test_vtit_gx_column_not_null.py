import polars as pl
import pandas as pd
import great_expectations as gx
from vtit_gx import *


def main():

    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": [None, "Alice" , None],
            "age": [25, 30, 40]
        }
    )
    config = {
        "column": "name",
    }
    r = gx_check_column_not_null(df,config)
    print(r)
if __name__ == "__main__":
    main()
