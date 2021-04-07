use gmall;

with tmp_start as (
    select mid_id,
           brand,
           model,
           count(*) login_count
    from dwd_start_log
    where dt = '2020-06-14'
    group by mid_id, brand, model
),
     tmp_page as (
         select mid_id,
                brand,
                model,
                collect_set(named_struct('page_id', page_id, 'page_count', page_count)) page_stats
         from (
                  select mid_id,
                         brand,
                         model,
                         page_id,
                         count(*) page_count
                  from dwd_page_log
                  where dt = '2020-06-14'
                  group by mid_id, brand, model, page_id
              ) tmp
         group by mid_id, brand, model
     )
select *
from tmp_start,
     tmp_page;


with tmp_login as (
    select user_id,
           count(*) login_count
    from dwd_start_log
    where dt = '2020-06-14'
      and user_id is not null
    group by user_id
),
     tmp_cart_as as (
         select user_id,
                count(*) cart_count
         from dwd_action_log
         where dt = '2020-06-14'
           and user_id is not null
           and action_id = 'cart_add'
         group by user_id
     ),
     tmp_order as (
         select user_id,
                count(*)                order_count,
                sum(final_total_amount) order_amount
         from dwd_fact_order_info
         where dt = '2020-06-14'
         group by user_id
     ),
     tmp_payment as (
         select user_id,
                count(*)            payment_count,
                sum(payment_amount) payment_amount
         from dwd_fact_payment_info
         where dt = '2020-06-14'
         group by user_id
     ),
     tmp_order_detail as (
         select user_id,
     )

select *
from tmp_payment;


select mid_id,
       brand,
       model,
       collect_set(named_struct('page_id', page_id, 'page_count', page_count)) page_stats
from (
         select mid_id,
                brand,
                model,
                page_id,
                count(*) page_count
         from dwd_page_log
         where dt = '2020-06-14'
         group by mid_id, brand, model, page_id
     ) tmp
group by mid_id, brand, model;



with tmp_page as (
    select mid_id,
           brand,
           model,
           collect_set(named_struct('page_id', page_id, 'page_count', page_count)) page_stats
    from (
             select mid_id,
                    brand,
                    model,
                    page_id,
                    count(*) page_count
             from dwd_page_log
             where dt = '2020-06-14'
             group by mid_id, brand, model, page_id
         ) tmp
    group by mid_id, brand, model
)
select *
from tmp_page
;

select user_id,
       sku_id,
       sum(sku_num)                                sku_num,
       count(*)                                    order_count,
       cast(sum(final_amount_d) as decimal(20, 2)) order_amount
from dwd_fact_order_detail
where dt = '2020-06-14'
group by user_id, sku_id;



select `if`(100 is not NULL, 'a=100', 'a=99');

