from pybaseball import statcast_batter, statcast_pitcher
import pandas as pd

def get_batter_data(player_id):
    df = statcast_batter('2023-04-01', '2023-10-01', player_id)
    return df

def get_pitcher_data(player_id):
    df = statcast_pitcher('2023-04-01', '2023-10-01', player_id)
    return df

def hr_features(df):
    df["is_hr"] = df["events"] == "home_run"
    return df

def strikeout_features(df):
    df["is_k"] = df["events"] == "strikeout"
    return df
