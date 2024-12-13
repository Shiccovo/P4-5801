import os
import unittest
import pandas as pd
from pathlib import Path
from core.py.scheduler import Scheduler

class TestScheduler(unittest.TestCase):
    @classmethod
    def setUp(cls):
        # set test directories and files before running tests
        cls.test_case_dir = Path("./data/test_case")
        cls.test_case_dir.mkdir(parents=True, exist_ok=True)

        # mock data for teams, venues, and leagues
        team_data = {
            "teamId": [1, 2],
            "name": ["Team A", "Team B"],
            "leagueId": [1, 1],
            "d1Start": [8, 9],
            "d1End": [16, 17]
        }
        venue_data = {
            "venueId": [1],
            "name": ["Venue 1"],
            "field": [1],
            "d1Start": [8],
            "d1End": [18]
        }
        league_data = {
            "leagueId": [1],
            "leagueName": ["League 1"],
            "seasonStart": [1],
            "seasonEnd": [2],
            "numberOfGames": [2],
            "seasonYear": [2024]
        }
        # save to CSV
        pd.DataFrame(team_data).to_csv(cls.test_case_dir / "team.csv", index=False)
        pd.DataFrame(venue_data).to_csv(cls.test_case_dir / "venue.csv", index=False)
        pd.DataFrame(league_data).to_csv(cls.test_case_dir / "league.csv", index=False)
        
    def test_scheduler_run(self):
        scheduler = Scheduler()
        exit_code = scheduler.run("test_case")
        self.assertEqual(exit_code, 0, "Scheduler did not exit with code 0")
        # verify output files
        csv_path = self.test_case_dir / "schedule.csv"
        json_path = self.test_case_dir / "schedule.json"
        self.assertTrue(csv_path.exists(), "Schedule CSV file was not created")
        self.assertTrue(json_path.exists(), "Schedule JSON file was not created")
        # verify contents of schedule file
        schedule_df = pd.read_csv(csv_path)
        self.assertGreater(len(schedule_df), 0, "No games were scheduled")
        self.assertIn("team1Name", schedule_df.columns, "Schedule file is missing 'team1Name' column")
        self.assertIn("team2Name", schedule_df.columns, "Schedule file is missing 'team2Name' column")

if __name__ == "__main__":
    unittest.main()
