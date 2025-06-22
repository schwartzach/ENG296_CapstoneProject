import scrapy

class PlayersSpider(scrapy.Spider):
    name = "rb_rushing"
    allowedDomain = ["pro-football-reference.com"]

    def __init__(self):
        self.players_data = {}
        self.completed_players = set()
        self.final_items = []

    def start_requests(self):
        for year in range (2014, 2025):
            url1 = f"https://www.pro-football-reference.com/years/{year}/rushing.htm"
            yield scrapy.Request(
                url=url1,
                callback=self.parse_rushing,
                meta={"year": year}
            )
            
            if year > 2017:
                url2 = f"https://www.pro-football-reference.com/years/{year}/rushing_advanced.htm"
                yield scrapy.Request(
                    url=url2,
                    callback=self.parse_advanced_rushing,
                    meta={"year": year}
                )
            
    def parse_rushing(self, response):
        year=response.meta['year']
        for row in response.css("table#rushing tbody tr"):
            print(row.extract()) #debug
            player=row.css("td[data-stat='name_display'] a::text").get()
            if not player:
                continue
            key = (player, year)
            if key not in self.players_data:
                self.players_data[key] = {}
            self.players_data[key].update({
                "player": player,
                "year": year,
                "age": row.css("td[data-stat='age']::text").get(),
                "team": row.css("td[data-stat='team_name_abbr'] a::text").get(),
                "position": row.css("td[data-stat='pos']::text").get(),
                "games": row.css("td[data-stat='games']::text").get(),
                "games_started": row.css("td[data-stat='games_started']::text").get(),
                "rush_att": row.css("td[data-stat='rush_att']::text").get(),
                "rush_yds": row.css("td[data-stat='rush_yds']::text").get(),
                "rush_td": row.css("td[data-stat='rush_td']::text").get(),
                "rush_success": row.css("td[data-stat='rush_success']::text").get(),
                "rush_long": row.css("td[data-stat='rush_long']::text").get(),
                "rush_yd_att": row.css("td[data-stat='rush_yds_per_att']::text").get(),
                "rush_yd_game": row.css("td[data-stat='rush_yds_per_g']::text").get(),
                "rush_att_game": row.css("td[data-stat='rush_att_per_g']::text").get(),
                "fumbles": row.css("td[data-stat='fumbles']::text").get(),
                "awards": row.css("td[data-stat='awards'] > a::text").get()
            }) 
            if year <=2017 and key not in self.completed_players:
                self.completed_players.add(key)
                yield self.players_data[key]
    def parse_advanced_rushing(self, response):
        year=response.meta['year']
        for row in response.css("table#adv_rushing tbody tr"):
            print(row.extract()) 
            player = row.css("td[data-stat='name_display'] a::text").get() 
            if not player:
                continue
            key = (player,year)
            if key not in self.players_data:
                self.players_data[key] = {}
            self.players_data[key].update({
                "player":player,
                "year": year,
                 "age": row.css("td[data-stat='age']::text").get(),
                "team": row.css("td[data-stat='team_name_abbr'] > a::text").get(),
                "position": row.css("td[data-stat='pos']::text").get(),
                "rush_b4_contact": row.css("td[data-stat='rush_yds_before_contact']::text").get(),
                "rush_att_avg_b4_contact": row.css("td[data-stat='rush_yds_bc_per_rush']::text").get(),
                "rush_yac": row.css("td[data-stat='rush_yac']::text").get(),
                "rush_yac_avg": row.css("td[data-stat='rush_yac_per_rush']::text").get(),
                "rush_brokenTackles": row.css("td[data-stat='rush_broken_tackles']::text").get(),
                "rush_brokenTackles_avg": row.css("td[data-stat='rush_broken_tackles_per_rush']::text").get()
            })
            yield self.players_data[key]
    def closed(self, reason):
        for data in self.players_data.values():
            self.final_items.append(data)
        if reason == "finished":
            self.logger.info("Spider completed as expected")
        elif reason == "cancelled":
            self.logger.warning("Spider got manually cancelled")
        elif reason == "shutdown":
            self.logger.warning("Spider engine being shut down")

