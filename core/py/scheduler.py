import sys
import pandas as pd
from itertools import combinations
from core.py.interval_tree import IntervalTree, Interval
from core.py.game import Game

class Scheduler:
    @staticmethod
    def run(case: str = "case1") -> int:
        """
        Main method to run the scheduling process for a given case.

        Args:
            case (str): Identifier for the specific case/data set to use. Defaults to "case1".

        Returns:
            int: Exit code indicating success (0) or failure (1).
        """
        # Define the input directory based on the provided case
        input_dir = f"./data/{case}/"
        input_teams = f"{input_dir}/team.csv"
        input_venues = f"{input_dir}/venue.csv"
        input_leagues = f"{input_dir}/league.csv"

        try:
            # Read team, venue, and league data from CSV files into pandas DataFrames
            team_df = pd.read_csv(input_teams)
            venue_df = pd.read_csv(input_venues)
            league_df = pd.read_csv(input_leagues)
        except Exception as e:
            # Handle any errors that occur during file reading
            print(f"Error reading input files: {e}")
            return 1  # Exit with error code

        # Initialize interval trees for each venue field to track their availability
        venue_trees = {}
        for _, v in venue_df.iterrows():
            # Create a unique key for each venue field combination
            key = f"{v['venueId']}_field_{v['field']}"
            venue_trees[key] = IntervalTree()

        # Initialize interval trees for each team to track their availability
        team_trees = {tid: IntervalTree() for tid in team_df['teamId']}

        all_scheduled_games = []  # List to store all successfully scheduled games

        # Iterate through each league to schedule its games
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
                # If neither column is present, log a message and skip this league
                print(f"League {league_name} missing 'seasonYear' or 'season' column.")
                continue  # Skip to the next league

            # Filter teams that belong to the current league
            league_teams = team_df[team_df['leagueId'] == league_id]
            team_ids = league_teams['teamId'].tolist()
            team_names = league_teams.set_index('teamId')['name'].to_dict()
            number_of_teams = len(team_ids)

            # Calculate the total number of required games for the league
            required_total_games = number_of_games_per_team * number_of_teams // 2

            print(f"\nScheduling for League: {league_name}")
            print(f"Number of Teams: {number_of_teams}, Required Games: {required_total_games}")

            # Check if there are enough teams to schedule games
            if number_of_teams < 2:
                print(f"Not enough teams in League: {league_name} to schedule games. Skipping...")
                continue  # Skip to the next league

            # Generate all possible unique pairs of teams for scheduling
            possible_pairs = list(combinations(team_ids, 2))
            total_possible_pairs = len(possible_pairs)

            if total_possible_pairs == 0:
                print(f"No possible pairs for League: {league_name}. Skipping...")
                continue  # Skip to the next league

            # Determine how many games should be scheduled per pair
            games_per_pair = required_total_games // total_possible_pairs
            extra_games = required_total_games % total_possible_pairs

            print(f"Games per pair: {games_per_pair}, Extra games to distribute: {extra_games}")

            # Create a list of game pairs to be scheduled based on the calculated distribution
            game_pairs = []
            for pair in possible_pairs:
                for _ in range(games_per_pair):
                    game_pairs.append(pair)
            # Distribute any extra games to the initial pairs
            for i in range(extra_games):
                game_pairs.append(possible_pairs[i])

            print(f"Total game pairs to schedule: {len(game_pairs)}")

            # Initialize a dictionary to keep track of how many times each pair has been scheduled
            pair_game_count = {pair: 0 for pair in possible_pairs}

            unscheduled_pairs = []  # List to keep track of pairs that couldn't be scheduled

            # Attempt to schedule each game pair
            for pair in game_pairs:
                team1_id, team2_id = pair
                team1_name = team_names[team1_id]
                team2_name = team_names[team2_id]

                scheduled = False  # Flag to indicate if the current pair has been scheduled

                # Iterate through each week in the season
                for week in range(season_start, season_end + 1):
                    if scheduled:
                        break  # Exit the week loop if the game has been scheduled

                    # Iterate through each day of the week (1 to 7)
                    for day in range(1, 8):
                        if scheduled:
                            break  # Exit the day loop if the game has been scheduled

                        # Retrieve the team's availability for the current day
                        team1 = league_teams[league_teams['teamId'] == team1_id].iloc[0]
                        team2 = league_teams[league_teams['teamId'] == team2_id].iloc[0]

                        t1_start = team1.get(f"d{day}Start", 0)  # Team 1 start availability
                        t1_end = team1.get(f"d{day}End", 24)    # Team 1 end availability
                        t2_start = team2.get(f"d{day}Start", 0)  # Team 2 start availability
                        t2_end = team2.get(f"d{day}End", 24)    # Team 2 end availability

                        # Determine the overlapping available time window for both teams
                        day_start = max(t1_start, t2_start)
                        day_end = min(t1_end, t2_end)

                        # Ensure there is enough time to schedule a 2-hour game
                        if day_end - day_start < 2:
                            continue  # Skip to the next day if insufficient time

                        # Generate potential start times in half-hour increments within the available window
                        potential_starts = [x * 0.5 for x in range(int(day_start * 2), int((day_end - 2) * 2) + 1)]

                        # Iterate through each potential start time to find a suitable slot
                        for start_time in potential_starts:
                            end_time = start_time + 2  # Calculate the end time based on a 2-hour game duration

                            # Create interval objects to represent the game time for both teams
                            interval_team1 = Interval(start_time, end_time, day, week)
                            interval_team2 = Interval(start_time, end_time, day, week)

                            # Check if either team is already busy during the proposed time
                            if team_trees[team1_id].overlap(interval_team1) or team_trees[team2_id].overlap(interval_team2):
                                continue  # Skip to the next start time if either team is unavailable

                            # Iterate through all venues to find an available field
                            for _, venue in venue_df.iterrows():
                                v_key = f"{venue['venueId']}_field_{venue['field']}"
                                v_start = venue.get(f"d{day}Start", 0)  # Venue start availability
                                v_end = venue.get(f"d{day}End", 24)    # Venue end availability

                                # Check if the venue is available during the proposed game time
                                if start_time < v_start or end_time > v_end:
                                    continue  # Venue is unavailable at this time

                                # Create an interval object to represent the venue's occupied time
                                interval_venue = Interval(start_time, end_time, day, week)

                                # Check if the venue is already booked during the proposed time
                                if venue_trees[v_key].overlap(interval_venue):
                                    continue  # Venue is busy, try the next venue

                                # If both teams and the venue are available, schedule the game
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

                                # Update the interval trees to mark the time as occupied for both teams and the venue
                                team_trees[team1_id].insert(interval_team1)
                                team_trees[team2_id].insert(interval_team2)
                                venue_trees[v_key].insert(interval_venue)

                                # Add the scheduled game to the list of all scheduled games
                                all_scheduled_games.append(game)
                                pair_game_count[pair] += 1  # Increment the scheduled count for this pair
                                scheduled = True  # Set the flag indicating the game has been scheduled

                                # Log the successful scheduling of the game
                                print(f"Scheduled: {team1_name} vs {team2_name} on Week {week}, Day {day}, {venue['name']} Field #{venue['field']} at {start_time}")
                                break  # Exit the venue loop as the game has been scheduled

                            if scheduled:
                                break  # Exit the start time loop as the game has been scheduled

                if not scheduled:
                    # If the game couldn't be scheduled, log the unscheduled pair
                    print(f"Could not schedule game between {team1_name} and {team2_name}")
                    unscheduled_pairs.append(pair)

            # After initial scheduling, verify if all required games have been scheduled
            for _, league in league_df.iterrows():
                league_id = league['leagueId']
                league_name = league['leagueName']
                number_of_teams = team_df[team_df['leagueId'] == league_id].shape[0]
                required_total_games = league['numberOfGames'] * number_of_teams // 2

                # Filter scheduled games that belong to the current league
                scheduled_games_league = [game for game in all_scheduled_games if game.league == league_name]
                total_scheduled_games = len(scheduled_games_league)

                print(f"\nTotal Required Games for League {league_name}: {required_total_games}")
                print(f"Total Scheduled Games for League {league_name}: {total_scheduled_games}")

                if total_scheduled_games < required_total_games:
                    # If not all games have been scheduled, attempt to schedule the remaining games
                    print(f"Attempting to schedule remaining {required_total_games - total_scheduled_games} games for League {league_name}...")
                    
                    # Identify the unscheduled pairs that belong to the current league
                    unscheduled_pairs_league = [pair for pair in unscheduled_pairs if pair[0] in team_ids and pair[1] in team_ids]

                    for pair in unscheduled_pairs_league:
                        team1_id, team2_id = pair
                        team1_name = team_names[team1_id]
                        team2_name = team_names[team2_id]

                        scheduled = False  # Reset the scheduled flag for the retry

                        # Iterate through weeks to find available slots for the remaining games
                        for week in range(season_start, season_end + 1):
                            if scheduled:
                                break  # Exit the week loop if the game has been scheduled

                            # Iterate through each day of the week
                            for day in range(1, 8):
                                if scheduled:
                                    break  # Exit the day loop if the game has been scheduled

                                # Retrieve the teams' availability for the current day
                                team1 = team_df[team_df['teamId'] == team1_id].iloc[0]
                                team2 = team_df[team_df['teamId'] == team2_id].iloc[0]

                                t1_start = team1.get(f"d{day}Start", 0)
                                t1_end = team1.get(f"d{day}End", 24)
                                t2_start = team2.get(f"d{day}Start", 0)
                                t2_end = team2.get(f"d{day}End", 24)

                                # Determine the overlapping available time window for both teams
                                day_start = max(t1_start, t2_start)
                                day_end = min(t1_end, t2_end)

                                # Ensure there is enough time to schedule a 2-hour game
                                if day_end - day_start < 2:
                                    continue  # Skip to the next day if insufficient time

                                # Generate potential start times in half-hour increments
                                potential_starts = [x * 0.5 for x in range(int(day_start * 2), int((day_end - 2) * 2) + 1)]

                                # Iterate through each potential start time to find a suitable slot
                                for start_time in potential_starts:
                                    end_time = start_time + 2  # Calculate end time based on a 2-hour game duration

                                    # Create interval objects to represent the game time for both teams
                                    interval_team1 = Interval(start_time, end_time, day, week)
                                    interval_team2 = Interval(start_time, end_time, day, week)

                                    # Check if either team is already busy during the proposed time
                                    if team_trees[team1_id].overlap(interval_team1) or team_trees[team2_id].overlap(interval_team2):
                                        continue  # Skip to the next start time if either team is unavailable

                                    # Iterate through all venues to find an available field
                                    for _, venue in venue_df.iterrows():
                                        v_key = f"{venue['venueId']}_field_{venue['field']}"
                                        v_start = venue.get(f"d{day}Start", 0)  # Venue start availability
                                        v_end = venue.get(f"d{day}End", 24)    # Venue end availability

                                        # Check if the venue is available during the proposed game time
                                        if start_time < v_start or end_time > v_end:
                                            continue  # Venue is unavailable at this time

                                        # Create an interval object to represent the venue's occupied time
                                        interval_venue = Interval(start_time, end_time, day, week)

                                        # Check if the venue is already booked during the proposed time
                                        if venue_trees[v_key].overlap(interval_venue):
                                            continue  # Venue is busy, try the next venue

                                        # If both teams and the venue are available, schedule the game
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

                                        # Update the interval trees to mark the time as occupied for both teams and the venue
                                        team_trees[team1_id].insert(interval_team1)
                                        team_trees[team2_id].insert(interval_team2)
                                        venue_trees[v_key].insert(interval_venue)

                                        # Add the scheduled game to the list of all scheduled games
                                        all_scheduled_games.append(game)
                                        pair_game_count[pair] += 1  # Increment the scheduled count for this pair
                                        scheduled = True  # Set the flag indicating the game has been scheduled

                                        # Log the successful scheduling of the game
                                        print(f"Re-scheduled: {team1_name} vs {team2_name} on Week {week}, Day {day}, {venue['name']} Field #{venue['field']} at {start_time}")
                                        break  # Exit the venue loop as the game has been scheduled

                                    if scheduled:
                                        break  # Exit the start time loop as the game has been scheduled

                        if not scheduled:
                            # If the game still couldn't be scheduled after retrying, log the unscheduled pair
                            print(f"Still could not schedule game between {team1_name} and {team2_name}")

                # After attempting to schedule all required games, verify the final count
                scheduled_games_league = [game for game in all_scheduled_games if game.league == league_name]
                total_scheduled_games = len(scheduled_games_league)
                print(f"Total Scheduled Games for League {league_name}: {total_scheduled_games} / {required_total_games}")

                if total_scheduled_games < required_total_games:
                    # If not all games could be scheduled, log a warning
                    print(f"Warning: Only {total_scheduled_games} out of {required_total_games} games were scheduled for League {league_name}.")
                    # Placeholder for additional retry or optimization logic
                    # Currently, the process continues and records the already scheduled games

        # Prepare the final scheduling records for output
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

        # Convert the scheduling records to a pandas DataFrame
        schedule_df = pd.DataFrame(schedule_records)

        # Write the schedule to CSV and JSON files within the input directory
        schedule_df.to_csv(f"{input_dir}/schedule.csv", index=False)
        schedule_df.to_json(f"{input_dir}/schedule.json", orient="records")
        print(f"\nScheduling complete. Output written to {input_dir}/schedule.csv and {input_dir}/schedule.json")

        return 0  # Exit with success code

if __name__ == "__main__":
    # Allow the user to specify the case as a command-line argument; default to "case1" if not provided
    case = sys.argv[1] if len(sys.argv) > 1 else "case1"
    scheduler = Scheduler()
    exit_code = scheduler.run(case)
    sys.exit(exit_code)
