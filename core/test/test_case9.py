import pytest
import pandas as pd
from core.py.scheduler import Scheduler

def test_case9():
    case = "case9"
    scheduler = Scheduler()
    scheduler.run(case)
    df = pd.read_csv(f"./data/{case}/schedule.csv")

    # assert there are enough games for double-headers every week
    # 6-team league, so each team plays 2 games per week (double-headers)
    # total games = (number of teams / 2) * 2 games per week * number of weeks
    number_of_teams = 6
    games_per_team_per_week = 2
    weeks_in_season = 5  # 5-week season
    expected_games = (number_of_teams // 2) * games_per_team_per_week * weeks_in_season

    assert len(df) == expected_games

    # additional checks: make sure each team appears the correct number of times for fairness
    team_games = pd.concat([df['team1Name'], df['team2Name']]).value_counts()
    expected_games_per_team = games_per_team_per_week * weeks_in_season

    for count in team_games.items():
        assert count == expected_games_per_team