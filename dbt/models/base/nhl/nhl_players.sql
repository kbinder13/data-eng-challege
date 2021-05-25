select
  	nhl_player_id as id,
	full_name,
	game_team_name as team_name,
	SUM(stats_assists) as assists,
	SUM(stats_goals) as goals,
	SUM(stats_assists) + SUM(stats_goals) as points
from {{ ref('player_game_stats') }}
group by 1,2,3
