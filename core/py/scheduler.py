import sys
import pandas as pd
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

        try:
            team_df = pd.read_csv(input_teams)
            venue_df = pd.read_csv(input_venues)
            league_df = pd.read_csv(input_leagues)
        except Exception as e:
            print(f"Error reading input files: {e}")
            return 1

        # Initialize the venue's time tree
        venue_trees = {}
        for _, v in venue_df.iterrows():
            key = f"{v['venueId']}_field_{v['field']}"
            venue_trees[key] = IntervalTree()

        # Initialize the team's time tree
        team_trees = {tid: IntervalTree() for tid in team_df['teamId']}

        all_scheduled_games = []  # Used to store all league games

        for _, league in league_df.iterrows():
            league_id = league['leagueId']
            league_name = league['leagueName']
            season_start = league['seasonStart']
            season_end = league['seasonEnd']
            number_of_games_per_team = league['numberOfGames']
            # Handle inconsistent column names for 'seasonYear' and 'season'
            if 'seasonYear' in league:
                season_year = league['seasonYear']
            elif 'season' in league:
                season_year = league['season']
            else:
                print(f"League {league_name} missing 'seasonYear' or 'season' column.")
                continue  # Skip this league

            # Filter teams of the current league
            league_teams = team_df[team_df['leagueId'] == league_id]
            team_ids = league_teams['teamId'].tolist()
            team_names = league_teams.set_index('teamId')['name'].to_dict()
            number_of_teams = len(team_ids)
            required_total_games = number_of_games_per_team * number_of_teams // 2

            print(f"\nScheduling for League: {league_name}")
            print(f"Number of Teams: {number_of_teams}, Required Games: {required_total_games}")

            if number_of_teams < 2:
                print(f"Not enough teams in League: {league_name} to schedule games. Skipping...")
                continue

            # Generate all possible unique pairs
            possible_pairs = list(combinations(team_ids, 2))
            total_possible_pairs = len(possible_pairs)

            if total_possible_pairs == 0:
                print(f"No possible pairs for League: {league_name}. Skipping...")
                continue

            # Calculate the number of games required per pair
            games_per_pair = required_total_games // total_possible_pairs
            extra_games = required_total_games % total_possible_pairs

            print(f"Games per pair: {games_per_pair}, Extra games to distribute: {extra_games}")

            # Create a list of game pairs to be scheduled
            game_pairs = []
            for pair in possible_pairs:
                for _ in range(games_per_pair):
                    game_pairs.append(pair)
            # Distribute extra games
            for i in range(extra_games):
                game_pairs.append(possible_pairs[i])

            print(f"Total game pairs to schedule: {len(game_pairs)}")

            # Initialize the scheduling count for game pairs
            pair_game_count = {pair: 0 for pair in possible_pairs}

            # Record unscheduled game pairs
            unscheduled_pairs = []

            # Attempt to schedule each game pair
            for pair in game_pairs:
                team1_id, team2_id = pair
                team1_name = team_names[team1_id]
                team2_name = team_names[team2_id]

                scheduled = False
                # Iterate through weeks, days, and venues sequentially
                for week in range(season_start, season_end + 1):
                    if scheduled:
                        break
                    for day in range(1, 8):
                        if scheduled:
                            break

                        # Get the teams' available times
                        team1 = league_teams[league_teams['teamId'] == team1_id].iloc[0]
                        team2 = league_teams[league_teams['teamId'] == team2_id].iloc[0]

                        t1_start = team1.get(f"d{day}Start", 0)
                        t1_end = team1.get(f"d{day}End", 24)
                        t2_start = team2.get(f"d{day}Start", 0)
                        t2_end = team2.get(f"d{day}End", 24)

                        # Find the intersection of both teams' available times
                        day_start = max(t1_start, t2_start)
                        day_end = min(t1_end, t2_end)

                        if day_end - day_start < 2:
                            continue  # Not enough time to schedule the game

                        # Generate potential start times in half-hour increments
                        potential_starts = [x * 0.5 for x in range(int(day_start * 2), int((day_end - 2) * 2) + 1)]

                        for start_time in potential_starts:
                            end_time = start_time + 2

                            # Check if both teams are free
                            interval_team1 = Interval(start_time, end_time, day, week)
                            interval_team2 = Interval(start_time, end_time, day, week)

                            if team_trees[team1_id].overlap(interval_team1) or team_trees[team2_id].overlap(interval_team2):
                                continue  # Teams are busy

                            # Iterate through venues in order
                            for _, venue in venue_df.iterrows():
                                v_key = f"{venue['venueId']}_field_{venue['field']}"
                                v_start = venue.get(f"d{day}Start", 0)
                                v_end = venue.get(f"d{day}End", 24)

                                if start_time < v_start or end_time > v_end:
                                    continue  # Venue is unavailable at this time

                                interval_venue = Interval(start_time, end_time, day, week)

                                if venue_trees[v_key].overlap(interval_venue):
                                    continue  # Venue is busy

                                # Schedule the game
                                game = Game(
                                    team1_id=team1_id,
                                    team2_id=team2_id,
                                    team1_name=team1_name,
                                    team2_name=team2_name,
                                    week=week,
                                    day=day,
                                    season=season_year,
                                    start=start_time,
                                    end=end_time,
                                    league=league_name,
                                    venue_name=f"{venue['name']} Field #{venue['field']}"
                                )

                                # Insert the time intervals
                                team_trees[team1_id].insert(interval_team1)
                                team_trees[team2_id].insert(interval_team2)
                                venue_trees[v_key].insert(interval_venue)

                                all_scheduled_games.append(game)
                                pair_game_count[pair] += 1
                                scheduled = True
                                print(f"Scheduled: {team1_name} vs {team2_name} on Week {week}, Day {day}, {venue['name']} Field #{venue['field']} at {start_time}")
                                break  # Game has been scheduled, exit venue loop

                            if scheduled:
                                break  # Game has been scheduled, exit time loop

                if not scheduled:
                    print(f"Could not schedule game between {team1_name} and {team2_name}")
                    unscheduled_pairs.append(pair)

            # Check if all games have been scheduled
            # Here we calculate each league's games and ensure the number of games meets the requirements
            for _, league in league_df.iterrows():
                league_id = league['leagueId']
                league_name = league['leagueName']
                number_of_teams = team_df[team_df['leagueId'] == league_id].shape[0]
                required_total_games = league['numberOfGames'] * number_of_teams // 2

                scheduled_games_league = [game for game in all_scheduled_games if game.league == league_name]
                total_scheduled_games = len(scheduled_games_league)

                print(f"\nTotal Required Games for League {league_name}: {required_total_games}")
                print(f"Total Scheduled Games for League {league_name}: {total_scheduled_games}")

                if total_scheduled_games < required_total_games:
                    print(f"Attempting to schedule remaining {required_total_games - total_scheduled_games} games for League {league_name}...")
                    # Get unscheduled game pairs
                    unscheduled_pairs_league = [pair for pair in unscheduled_pairs if pair[0] in team_ids and pair[1] in team_ids]

                    for pair in unscheduled_pairs_league:
                        team1_id, team2_id = pair
                        team1_name = team_names[team1_id]
                        team2_name = team_names[team2_id]

                        scheduled = False
                        # Iterate through weeks, days, and venues sequentially
                        for week in range(season_start, season_end + 1):
                            if scheduled:
                                break
                            for day in range(1, 8):
                                if scheduled:
                                    break

                                # Get the teams' available times
                                team1 = team_df[team_df['teamId'] == team1_id].iloc[0]
                                team2 = team_df[team_df['teamId'] == team2_id].iloc[0]

                                t1_start = team1.get(f"d{day}Start", 0)
                                t1_end = team1.get(f"d{day}End", 24)
                                t2_start = team2.get(f"d{day}Start", 0)
                                t2_end = team2.get(f"d{day}End", 24)

                                # Find the intersection of both teams' available times
                                day_start = max(t1_start, t2_start)
                                day_end = min(t1_end, t2_end)

                                if day_end - day_start < 2:
                                    continue  # Not enough time to schedule the game

                                # Generate potential start times in half-hour increments
                                potential_starts = [x * 0.5 for x in range(int(day_start * 2), int((day_end - 2) * 2) + 1)]

                                for start_time in potential_starts:
                                    end_time = start_time + 2

                                    # Check if both teams are free
                                    interval_team1 = Interval(start_time, end_time, day, week)
                                    interval_team2 = Interval(start_time, end_time, day, week)

                                    if team_trees[team1_id].overlap(interval_team1) or team_trees[team2_id].overlap(interval_team2):
                                        continue  # Teams are busy

                                    # Iterate through venues in order
                                    for _, venue in venue_df.iterrows():
                                        v_key = f"{venue['venueId']}_field_{venue['field']}"
                                        v_start = venue.get(f"d{day}Start", 0)
                                        v_end = venue.get(f"d{day}End", 24)

                                        if start_time < v_start or end_time > v_end:
                                            continue  # Venue is unavailable at this time

                                        interval_venue = Interval(start_time, end_time, day, week)

                                        if venue_trees[v_key].overlap(interval_venue):
                                            continue  # Venue is busy

                                        # Schedule the game
                                        game = Game(
                                            team1_id=team1_id,
                                            team2_id=team2_id,
                                            team1_name=team1_name,
                                            team2_name=team2_name,
                                            week=week,
                                            day=day,
                                            season=season_year,
                                            start=start_time,
                                            end=end_time,
                                            league=league_name,
                                            venue_name=f"{venue['name']} Field #{venue['field']}"
                                        )

                                        # Insert the time intervals
                                        team_trees[team1_id].insert(interval_team1)
                                        team_trees[team2_id].insert(interval_team2)
                                        venue_trees[v_key].insert(interval_venue)

                                        all_scheduled_games.append(game)
                                        pair_game_count[pair] += 1
                                        scheduled = True
                                        print(f"Re-scheduled: {team1_name} vs {team2_name} on Week {week}, Day {day}, {venue['name']} Field #{venue['field']} at {start_time}")
                                        break  # Game has been scheduled, exit venue loop

                                    if scheduled:
                                        break  # Game has been scheduled, exit time loop

                        if not scheduled:
                            print(f"Still could not schedule game between {team1_name} and {team2_name}")

                # Update the total number of scheduled games
                scheduled_games_league = [game for game in all_scheduled_games if game.league == league_name]
                total_scheduled_games = len(scheduled_games_league)
                print(f"Total Scheduled Games for League {league_name}: {total_scheduled_games} / {required_total_games}")

                if total_scheduled_games < required_total_games:
                    print(f"Warning: Only {total_scheduled_games} out of {required_total_games} games were scheduled for League {league_name}.")
                    # More complex retry or optimization logic can be implemented here
                    # Currently, we continue and record the already scheduled games

        # Prepare scheduling records
        schedule_records = []
        for g in all_scheduled_games:
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

        # Write to schedule.csv and schedule.json
        schedule_df = pd.DataFrame(schedule_records)
        schedule_df.to_csv(f"{input_dir}/schedule.csv", index=False)
        schedule_df.to_json(f"{input_dir}/schedule.json", orient="records")
        print(f"\nScheduling complete. Output written to {input_dir}/schedule.csv and {input_dir}/schedule.json")
        return 0

if __name__ == "__main__":
    case = sys.argv[1] if len(sys.argv) > 1 else "case1"
    scheduler = Scheduler()
    exit_code = scheduler.run(case)
    sys.exit(exit_code)
