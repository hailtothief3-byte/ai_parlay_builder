from nba_api.stats.endpoints import playergamelog
import pandas as pd

def get_player_games(player_id, season="2023-24"):
    gamelog = playergamelog.PlayerGameLog(
        player_id=player_id,
        season=season
    )
    df = gamelog.get_data_frames()[0]
    return df

def build_features(df):
    df = df.sort_values("GAME_DATE")
    
    df["PTS_LAST_5"] = df["PTS"].rolling(5).mean()
    df["AST_LAST_5"] = df["AST"].rolling(5).mean()
    df["REB_LAST_5"] = df["REB"].rolling(5).mean()
    
    return df.dropna()
