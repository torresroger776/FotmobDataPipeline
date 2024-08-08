create or replace table `epl-data-project.fotmob_data.looker_data` as (
select
ft.shot_id,
p.player_name,
t.team_name,
st.shot_type,
et.event_type,
et.situation,
ft.xG,
ft.xGOT,
ft.shot_from_x,
ft.shot_from_y,
ft.is_blocked,
ft.blocked_x,
ft.blocked_y,
ft.goal_crossed_y,
ft.goal_crossed_z

from
`epl-data-project.fotmob_data.fact_table` ft
join `epl-data-project.fotmob_data.match_dim` m on ft.match_id = m.match_id
join `epl-data-project.fotmob_data.player_dim` p on ft.player_id = p.player_id
join `epl-data-project.fotmob_data.team_dim` t on ft.team_id = t.team_id
join `epl-data-project.fotmob_data.shot_type_dim` st on ft.shot_type_id = st.shot_type_id
join `epl-data-project.fotmob_data.event_type_dim` et on ft.event_type_id = et.event_type_id
);