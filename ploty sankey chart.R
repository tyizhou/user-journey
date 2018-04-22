library(plotly) 
library(rjson) 
library('impaler') # impala connection
library('dplyr')

ds <- impaler$new() # environment to run impala query

## create sql query
# this query is applies on a session level table, each observation contains all the information of a user session
query <- "
with cte as (
select
    traffic_type_parent as tp,
    case
        when booking_amount>0 then 0 -- when leading to booking, then mark 'next_tp' 0 (booking)
        else
            case
                when lead(tracking_id,1) over(partition by tracking_id order by session_timestamp)=tracking_id then lead(traffic_type_parent,1) over(partition by tracking_id order by session_timestamp)
                else -1 -- if next session belongs to another tracking_id, then mark 'next_tp' -1 (no booking)
            end
    end as next_tp,
    bookings,
    booking_amount,
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
    count(*) as traffic -- or sum(bookings) etc. in other use cases
from (
        select
            concat(cast(tp as string),'-',cast(case -- name each touch point in form of 'traffic_type-step_of_journey
                                                when rank() over (partition by tracking_id order by session_timestamp)>5 and next_tp<>0 then 5 -- cap journey length for visualization
                                                when rank() over (partition by tracking_id order by session_timestamp)>5 and next_tp=0 then 6
                                                else rank() over (partition by tracking_id order by session_timestamp)
                                               end
                                               as string)) as tp,
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
            tp<>next_tp -- ignore same traffic type path
    ) a
group by 1,2
order by 1,2
"

## prepare parameters of sankey chart
cj <- ds$execute_query(query) # run impala query
label_sankey <- unique(c(as.vector(unique(cj$tp)),as.vector(unique(cj$next_tp)))) # all the touch point as label for sankey chart
cj_mod <- cj %>%
  mutate(source = match(tp, label_sankey)-1, # source position
         target = match(next_tp, label_sankey)-1) # target position

## plot sankey chart
p <- plot_ly(
  type = "sankey",
  orientation = "h",
  
  node = list(
    label = label_sankey,
    #color = c("blue", "blue", "blue", "blue", "blue", "blue"),
    pad = 15,
    thickness = 20,
    line = list(
      color = "black",
      width = 0.5
    )
  ),
  
  link = list(
    source = as.vector(cj_mod$source),
    target = as.vector(cj_mod$target),
    value = as.vector(cj_mod$traffic)
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
