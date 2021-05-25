with ranked as 
	(select 
		team_name,
		full_name,
		points,
		RANK() OVER (PARTITION BY team_name order by points desc) as rk
	from {{ ref('nhl_players') }}
	where points > 0 
	)
select team_name,
       full_name,
       points
from ranked
where rk = 1
