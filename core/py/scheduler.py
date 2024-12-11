import sys
import pandas as pd
import math
from itertools import combinations
from core.py.interval_tree import IntervalTree, Interval
from core.py.game import Game

class Scheduler:
    @staticmethod
    def run(case: str = "case1") -> int:
        input_dir = f"./data/{case}/"
        input_teams = f"{input_dir}/team.csv"
        input_venues = f"{input_dir}/venue.csv"
        input_leagues = f"{input_dir}/league.csv"

        # Load data
        try:
            team_df = pd.read_csv(input_teams)
            venue_df = pd.read_csv(input_venues)
            league_df = pd.read_csv(input_leagues)
        except Exception as e:
            print(f"Error reading input files: {e}")
            return 1

        # Build mappings
        team_map = team_df.set_index('teamId').to_dict(orient='index')
        league_map = league_df.set_index('leagueId').to_dict(orient='index')

        # Prepare interval trees
        # For teams
        team_trees = {tid: IntervalTree() for tid in team_df['teamId']}
        # For venues: key = "venueId_field"
        venue_trees = {}
        for _, v in venue_df.iterrows():
            key = f"{v['venueId']}_field_{v['field']}"
            venue_trees[key] = IntervalTree()

        games_scheduled = []

        # For each league, schedule the required games
        for _, league in league_df.iterrows():
            league_id = league['leagueId']
            league_name = league['leagueName']
            sport_id = league['sport']
            season_start = league['seasonStart']
            season_end = league['seasonEnd']
            number_of_games_per_team = league['numberOfGames']
            season_year = league['season']

            # Teams in this league
            league_teams = team_df[team_df['leagueId'] == league_id]
            team_ids = sorted(league_teams['teamId'].tolist())
            team_names = league_teams.set_index('teamId')['name'].to_dict()
            number_of_teams = len(team_ids)

            # Total games to schedule
            total_games = number_of_teams * number_of_games_per_team // 2

            # All pairs
            possible_pairs = list(combinations(team_ids, 2))
            # Sort pairs deterministically
            possible_pairs = sorted(possible_pairs, key=lambda x: (x[0], x[1]))

            # Distribute games among pairs
            games_per_pair = total_games // len(possible_pairs)
            extra_games = total_games % len(possible_pairs)

            game_pairs = []
            for pair in possible_pairs:
                # add the base number of games per pair
                game_pairs.extend([pair] * games_per_pair)
            # add the extra games to the first 'extra_games' pairs
            for i in range(extra_games):
                game_pairs.append(possible_pairs[i])

            # Count how many games each team has
            team_game_count = {tid: 0 for tid in team_ids}

            # Sort venues deterministically by (venueId, field)
            all_venues = venue_df.to_dict(orient='records')
            all_venues.sort(key=lambda x: (x['venueId'], x['field']))

            # Try to schedule each pair
            # Systematic approach: no randomization
            # Iterate pair by pair in the given order
            # For each pair, iterate week, day, try all venues and all start times
            for (team1_id, team2_id) in game_pairs:
                if team_game_count[team1_id] >= number_of_games_per_team or team_game_count[team2_id] >= number_of_games_per_team:
                    # already at max games for either team
                    continue

                team1 = team_map[team1_id]
                team2 = team_map[team2_id]

                scheduled = False
                # Try each week in [season_start, season_end]
                for week in range(season_start, season_end + 1):
                    if scheduled:
                        break
                    # Days from 1 to 7
                    for day in range(1, 8):
                        if scheduled:
                            break

                        # Compute team availability intersection
                        t1_start = team1.get(f"d{day}Start", 0.0)
                        t1_end = team1.get(f"d{day}End", 24.0)
                        t2_start = team2.get(f"d{day}Start", 0.0)
                        t2_end = team2.get(f"d{day}End", 24.0)

                        day_start = max(t1_start, t2_start)
                        day_end = min(t1_end, t2_end)

                        # Need at least 2 hours
                        if day_end - day_start < 2:
                            continue

                        # Potential start times every 30 minutes
                        # Convert to half-hour blocks
                        start_block = math.ceil(day_start * 2)
                        end_block = math.floor(day_end * 2)
                        potential_starts = []
                        for block in range(start_block, end_block + 1):
                            start_time = block * 0.5
                            end_time = start_time + 2.0
                            if end_time <= day_end:
                                potential_starts.append(start_time)

                        # Try each start time and each venue systematically
                        for start_time in potential_starts:
                            if scheduled:
                                break
                            end_time = start_time + 2.0

                            # Check team availability via their trees
                            interval_team1 = Interval(start_time, end_time, day, week)
                            interval_team2 = Interval(start_time, end_time, day, week)

                            if team_trees[team1_id].overlap(interval_team1) or team_trees[team2_id].overlap(interval_team2):
                                continue

                            # Try each venue
                            for v in all_venues:
                                v_start = v.get(f"d{day}Start", 0.0)
                                v_end = v.get(f"d{day}End", 24.0)
                                if start_time < v_start or end_time > v_end:
                                    # venue not available during this slot
                                    continue

                                key = f"{v['venueId']}_field_{v['field']}"
                                interval_venue = Interval(start_time, end_time, day, week)
                                if venue_trees[key].overlap(interval_venue):
                                    continue

                                # Found a slot
                                game = Game(
                                    team1_id=team1_id,
                                    team2_id=team2_id,
                                    team1_name=team_names[team1_id],
                                    team2_name=team_names[team2_id],
                                    week=week,
                                    day=day,
                                    season=season_year,
                                    start=start_time,
                                    end=end_time,
                                    league=league_name,
                                    venue_name=f"{v['name']} Field #{v['field']}"
                                )

                                # Insert intervals
                                team_trees[team1_id].insert(interval_team1)
                                team_trees[team2_id].insert(interval_team2)
                                venue_trees[key].insert(interval_venue)

                                games_scheduled.append(game)
                                team_game_count[team1_id] += 1
                                team_game_count[team2_id] += 1

                                scheduled = True
                                break

        # Output schedule
        if len(games_scheduled) == 0:
            print("No games scheduled, schedule.csv will be empty.")

        schedule_records = []
        for g in games_scheduled:
            schedule_records.append({
                "team1Name": g.team1_name,
                "team2Name": g.team2_name,
                "week": g.week,
                "day": g.day,
                "start": g.start,
                "end": g.end,
                "season": g.season,
                "league": g.league,
                "location": g.venue_name
            })

        schedule_df = pd.DataFrame(schedule_records)
        if schedule_df.empty:
            print("schedule.csv is empty after scheduling attempt.")
        schedule_df.to_csv(f"{input_dir}/schedule.csv", index=False)
        schedule_df.to_json(f"{input_dir}/schedule.json", orient="records")
        return 0

if __name__ == "__main__":
    case = sys.argv[1] if len(sys.argv) > 1 else "case1"
    scheduler = Scheduler()
    exit_code = scheduler.run(case)
    sys.exit(exit_code)
