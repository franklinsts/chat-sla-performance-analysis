with base_1 as (
    select 
        *,
        row_number() over (partition by platform_id, session_start_time order by is_productive desc) as rn,
        lead(action_type) over (partition by platform_id, session_start_time order by is_productive desc) as next_action
    from analytics.chatbot_interactions
),

base_chat_no_dup as (  
    select 
        case 
            when :granularity = 'Day' then date(session_start_time)
            when :granularity = 'Week' then date(date_trunc('week', session_start_time))
            when :granularity = 'Month' then date(date_trunc('month', session_start_time))
        end as granularity,
        *,
        case 
            when action_type = next_action then action_type
            when action_type like "closed" or next_action like "closed" then "closed"
            when action_type like "transferred" or next_action like "transferred" then "transferred"
            else action_type 
        end as new_action_type
    from base_1 
    where rn = 1 
),

queue_entries as (
    select 
        ticket_id, 
        queue_name, 
        entered_at
    from support.ticket_group_history
    left join support.ticket_details using (ticket_id)
    where queue_name in (
        'Level 1 - Payments',
        'Level 2 - Technical',
        'Level 1 - Investments',
        'Backoffice - Insurance',
        'Backoffice - Banking',
        'Legacy',
        'Complaints',
        'Social Media'
    )
    and lower(channel) like '%chat%'
),

first_agent_responses as (
    select 
        a.ticket_id,
        a.queue_name,
        a.first_agent_response_time,
        a.chat_session_id
    from analytics.chat_sessions a
    join base_chat_no_dup d 
        on a.ticket_id = d.platform_id 
        and d.session_id = a.chat_session_id
    where a.first_agent_response_time is not null
        and d.agent_email not like '%integration%'
),

paired_tickets as (
    select 
        c.ticket_id,
        c.queue_name,
        c.first_agent_response_time,
        c.chat_session_id,
        f.entered_at,
        row_number() over (
            partition by c.ticket_id, c.queue_name, c.first_agent_response_time, c.chat_session_id 
            order by f.entered_at desc
        ) as rn
    from first_agent_responses c
    join queue_entries f 
        on c.ticket_id = f.ticket_id 
        and c.queue_name = f.queue_name
    where f.entered_at < c.first_agent_response_time
),

base_offered as (
    select
        case 
            when :granularity = 'Day' then date(entered_at)
            when :granularity = 'Week' then date(date_trunc('week', entered_at))
            when :granularity = 'Month' then date(date_trunc('month', entered_at))
        end as granularity,
        queue_name,
        ticket_id
    from support.ticket_group_history
    left join support.ticket_details using (ticket_id)
    where queue_name in (
        'Level 1 - Payments',
        'Level 2 - Technical',
        'Level 1 - Investments',
        'Backoffice - Insurance',
        'Backoffice - Banking',
        'Legacy',
        'Complaints',
        'Social Media'
    )
    and lower(channel) like '%chat%'
),

base_te as (
    select 
        c.ticket_id, 
        f.entered_at, 
        c.session_start_time, 
        c.session_end_time, 
        c.queue_name,
        row_number() over (partition by c.ticket_id order by c.session_start_time, f.entered_at desc) as rn
    from analytics.chat_sessions c
    left join queue_entries f 
        on c.ticket_id = f.ticket_id 
        and c.queue_name = f.queue_name 
        and f.entered_at <= c.session_start_time
    where c.queue_name in (
        'Level 1 - Payments',
        'Level 2 - Technical',
        'Level 1 - Investments',
        'Backoffice - Insurance',
        'Backoffice - Banking',
        'Legacy',
        'Complaints',
        'Social Media'
    )
),

base_te_final as (
    select 
        ticket_id,
        entered_at,
        session_start_time,
        session_end_time,
        queue_name,
        case 
            when rn = 1 then (unix_timestamp(session_start_time) - unix_timestamp(entered_at)) / 60
            else null
        end as handling_time_minutes
    from base_te
    where rn = 1 
    and entered_at is not null
    and session_start_time is not null
),

base_first_response as (
    select 
        *,
        (unix_timestamp(first_agent_response_time) - unix_timestamp(entered_at)) / 60 as wait_time_per_queue
    from paired_tickets
    where rn = 1
),

base as (
    select 
        d.created_at,
        a.ticket_id,
        d.agent_email,
        a.chat_session_id,
        f.entered_at,
        a.session_start_time,
        a.session_end_time,
        a.first_agent_response_time,
        case a.queue_name
            when "Level 1 - Payments" then 'Payments'
            when "Backoffice - Insurance" then 'Insurance'
            when "Level 1 - Investments" then 'Investments'
            when "Complaints" then 'Complaints'
            when "Social Media" then 'Social Media'
            when "Legacy - Original" then 'Legacy'
            else null
        end as department,
        d.business_unit,
        a.queue_name as group_name,
        d.segment,
        d.product_type,
        d.is_productive,
        d.new_action_type as action_type,
        d.response_time_minutes as avg_response_time,
        case 
            when row_number() over (partition by a.ticket_id, f.entered_at order by a.first_agent_response_time asc) = 1 
            then f.wait_time_per_queue else null 
        end as wait_time_per_queue,
        d.handling_time_minutes,
        e.handling_time_minutes as te,
        case 
            when a.queue_name in ('Social Media') and te <= 60 and te is not null then 1 
            when te <= 4320 and te is not null then 1 
            when te is null then null
            else 0
        end as within_sla
    from analytics.chat_sessions a
    left join base_chat_no_dup d 
        on a.ticket_id = d.platform_id  
        and d.session_id = a.chat_session_id 
    left join base_first_response f 
        on a.ticket_id = f.ticket_id 
        and a.first_agent_response_time = f.first_agent_response_time 
        and a.chat_session_id = f.chat_session_id
    left join base_te_final e 
        on a.ticket_id = e.ticket_id 
        and a.session_start_time = e.session_start_time
    where a.queue_name in (
        'Level 1 - Payments','Level 2 - Technical','Level 1 - Investments',
        'Backoffice - Insurance','Backoffice - Banking',
        'Legacy - Original','Complaints','Social Media'
    )
    and d.agent_email not like '%integration%'
),

base_final as (
    select
        case 
            when :granularity = 'Day' then date(session_start_time)
            when :granularity = 'Week' then date(date_trunc('week', session_start_time))
            when :granularity = 'Month' then date(date_trunc('month', session_start_time))
        end as granularity,
        count(distinct ticket_id) as handled,
        sum(is_productive) as productive_actions,
        count(distinct case when is_productive = 1 then ticket_id end) as productivity,
        count(distinct agent_email) as headcount,
        sum(is_productive) / count(distinct agent_email) as productivity_per_agent,
        count(action_type) filter (where action_type = 'closed') as closed_volume,
        count(action_type) filter (where action_type = 'transferred') as transferred_volume,
        avg(avg_response_time) as avg_response_time,
        avg(handling_time_minutes) as avg_handling_time,
        sum(within_sla) filter (where within_sla is not null) as sla_compliance,
        sum(te) filter (where te is not null) as total_handling_time,
        count(wait_time_per_queue) filter (where wait_time_per_queue between 0 and 1440) as backlog_24h,
        count(wait_time_per_queue) filter (where wait_time_per_queue between 0 and 2880) as backlog_48h,
        count(wait_time_per_queue) filter (where wait_time_per_queue between 0 and 4320) as backlog_72h,
        count(wait_time_per_queue) filter (where wait_time_per_queue > 4320) as backlog_above_72h
    from base
    where created_at > '2025-01-01'
    group by 1
)

select 
    granularity, 
    case when :granularity like "Day" then (  
        case dayofweek(granularity)
            when 1 then "Sunday"
            when 2 then "Monday"
            when 3 then "Tuesday"
            when 4 then "Wednesday"
            when 5 then "Thursday"
            when 6 then "Friday"
            when 7 then "Saturday" end
        ) else :granularity end as day_of_week,

    count(b.ticket_id) as offered,
    sla_compliance / offered as ns,
    productive_actions,
    headcount, 
    productivity_per_agent, 
    handled / headcount as handled_per_agent,
    productivity,
    productivity / handled as productivity_ratio, 
    closed_volume, 
    transferred_volume, 
    avg_response_time, 
    avg_handling_time, 
    sla_compliance,
    backlog_24h, 
    backlog_48h, 
    backlog_72h,
    backlog_above_72h,
    total_handling_time / offered as avg_total_handling_time
from base_final 
left join base_offered b using (granularity)
where granularity between :date.min and :date.max
group by 1,2,5,6,7,8,9,10,11,12,13,14,15,16,17,18
order by 1 desc