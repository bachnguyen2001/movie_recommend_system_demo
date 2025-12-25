import pandas as pd


def leave_one_out_split(df: pd.DataFrame):
    """
    For each user:
    - last interaction → test
    - rest → train
    """

    df = df.sort_values(["userId", "timestamp"])

    test_df = df.groupby("userId").tail(1)
    train_df = df.drop(test_df.index)

    return train_df, test_df
