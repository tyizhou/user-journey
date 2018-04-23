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
    concat(path,'-',conversion) user_path,
    count(*) as traffic -- or sum(bookings) etc. in other use cases
from (
        select
            tracking_id,
            last_value(next_tp) over(partition by tracking_id order by session_timestamp asc) as conversion,
            group_concat(tp order by session_timestamp asc separator '-') as path
        from
            cte
        where
            tp<>next_tp -- ignore same traffic type path
    ) a
group by 1
order by 2 desc
