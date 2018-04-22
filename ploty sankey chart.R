library(plotly)
library(rjson)
library('impaler')
library('dplyr')

ds <- impaler$new()

query <- "with cte as (
select
    traffic_type_parent as tp,
case
when booking_amount>0 then 0 
else
case
when lead(tracking_id,1) over(partition by tracking_id order by session_timestamp)=tracking_id then lead(traffic_type_parent,1) over(partition by tracking_id order by session_timestamp)
else -1
end
end as next_tp,
bookings,
booking_amount
tracking_int_id,
tracking_id,
session_id,
session_timestamp,
is_bouncer,
ymd
from
trivago_analytic.session_stats_master
where
ymd>=20180101
and crawler_id=0
and cip_detail.cip_language='uk'
)
select
tp,
next_tp,
count(*) traffic
from (
select
concat(cast(tp as string),'-',cast(case
when rank() over (partition by tracking_id order by session_timestamp)>5 then 5
else rank() over (partition by tracking_id order by session_timestamp)
end
as string)) tp,
case
when next_tp in (0,-1) then cast(next_tp as string)
else concat(cast(next_tp as string),'-',cast(case
when rank() over (partition by tracking_id order by session_timestamp)>5 then 6
else rank() over (partition by tracking_id order by session_timestamp)+1
end
as string))
end as next_tp
from
cte
where
tp<>next_tp) a
group by 1,2
order by 1,2"

cj <- ds$execute_query(query)
label_sankey <- unique(c(as.vector(unique(cj$tp)),as.vector(unique(cj$next_tp))))
cj_mod <- cj %>%
  mutate(source = match(tp, label_sankey)-1,
         target = match(next_tp, label_sankey)-1)

cj_2 <- ds$execute_query(query)
label_sankey_2 <- unique(c(as.vector(unique(cj_2$tp)),as.vector(unique(cj_2$next_tp))))
cj_mod_2 <- cj_2 %>%
  mutate(source = match(tp, label_sankey_2)-1,
         target = match(next_tp, label_sankey_2)-1)

cj_3 <- filter(cj_2, next_tp != -1)
label_sankey_3 <- unique(c(as.vector(unique(cj_3$tp)),as.vector(unique(cj_3$next_tp))))
cj_mod_3 <- cj_3 %>%
  mutate(source = match(tp, label_sankey_3)-1,
         target = match(next_tp, label_sankey_3)-1)


p <- plot_ly(
  type = "sankey",
  orientation = "h",
  
  node = list(
    label = label_sankey_3,
    #color = c("blue", "blue", "blue", "blue", "blue", "blue"),
    pad = 15,
    thickness = 20,
    line = list(
      color = "black",
      width = 0.5
    )
  ),
  
  link = list(
    source = as.vector(cj_mod_3$source),
    target = as.vector(cj_mod_3$target),
    value = as.vector(cj_mod_3$traffic)
  )
) %>% 
  layout(
    title = "Basic Sankey Diagram",
    font = list(
      size = 10
    )
  )

# Create a shareable link to your chart
# Set up API credentials: https://plot.ly/r/getting-started
chart_link = api_create(p, filename="sankey-basic-example")
chart_link
p
