class Game:
    def __init__(self, team1_id, team2_id, team1_name, team2_name, week, day, season, start, end, league, venue_name):
        self.team1_id = team1_id
        self.team2_id = team2_id
        self.team1_name = team1_name
        self.team2_name = team2_name
        self.week = week
        self.day = day
        self.season = season
        self.start = start
        self.end = end
        self.league = league
        self.venue_name = venue_name

    def dump(self):
        print(f"Team {self.team1_name} vs. Team {self.team2_name} ({self.start}-{self.end}) Week {self.week}, Day {self.day} at {self.venue_name}")
