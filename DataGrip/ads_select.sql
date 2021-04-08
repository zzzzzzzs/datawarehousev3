use gmall;

-- ************************************************************
-- 商品统计的维度表


-- zs
-- 订单数和订单金额
select "2020-06-14",
       recent_days    recent_days,
       spu_id         spu_id,
       spu_name       spu_name,
       tm_id          tm_id,
       tm_name        tm_name,
       category3_id   category3_id,
       category3_name category3_name,
       category2_id   category2_id,
       category2_name category2_name,
       category1_id   category1_id,
       category1_name category1_name,
       sum(order_count),
       sum(order_amount)
from (
         select sku_id,
                recent_days,
                case
                    when recent_days = 1 then order_last_1d_count
                    when recent_days = 7 then order_last_7d_count
                    when recent_days = 30 then order_last_30d_count
                    end order_count,
                case
                    when recent_days = 1 then order_last_1d_final_amount
                    when recent_days = 7 then order_last_7d_final_amount
                    when recent_days = 30 then order_last_30d_final_amount
                    end order_amount
         from dwt_sku_topic lateral view explode(array(1, 7, 30)) tmp as recent_days
     ) t1
         left join
     (
--          这个是维度表
         select id,
                spu_id,
                spu_name,
                tm_id,
                tm_name,
                category3_id,
                category3_name,
                category2_id,
                category2_name,
                category1_id,
                category1_name
         from dim_sku_info
         where dt = "2020-06-14"
     ) t2
     on t1.sku_id = t2.id
group by recent_days, spu_id, spu_name, tm_id, tm_name, category3_id, category3_name, category2_id, category2_name,
         category1_id, category1_name
;

-- ************************************************************


-- ************************************************************
-- 品牌复购率

-- 每月品牌复购率 = 某品牌本月被购买的次数 / 所有品牌本月被购买的次数。(以订单为单位，不是购买次数)

select "2020-06-14" dt,
       recent_days,
       tm_id,
       tm_name,
       sum(order_user_count),
       sum(`if`(order_user_count >= 2, 1, 0)),
       sum(`if`(order_user_count >= 1, 1, 0)),
       cast(sum(if(order_user_count >= 2, 1, 0)) / sum(if(order_user_count >= 1, 1, 0)) as decimal(16, 2))
from (
         select recent_days,
                tm_id,
                tm_name,
                sum(order_user_count) order_user_count
         from (
                  select recent_days,
                         user_id,
                         sku_id,
                         -- 某品牌x天内被某个用户购买的次数
                         count(*) order_user_count
                  from dwd_order_detail lateral view explode(Array(1, 7, 30)) tmp as recent_days
                  where dt >= date_add("2020-06-14", -recent_days + 1)
                  group by recent_days, sku_id, user_id
              ) t1
                  left join (
             select id,
                    tm_id,
                    tm_name
             from dim_sku_info
             where dt = "2020-06-14"
         ) t2
                            on t1.sku_id = t2.id
         group by recent_days, tm_id, tm_name, user_id
     ) t3
group by recent_days, "2020-06-14", tm_id, tm_name
;

-- ************************************************************
-- 订单统计
-- TODO  : 可以反驳一波
select "2020-06-14",
       recent_deys,
       count(id)               order_count,
       sum(final_amount)       order_amount,
       count(distinct user_id) order_user_count
from dwd_order_info lateral view explode(array(1, 7, 30)) tmp as recent_deys
where dt >= date_add("2020-06-14", -recent_deys + 1)
group by recent_deys
;

-- 文档的答案，和我做的一样。
select '2020-06-14',
       recent_days,
       sum(order_count),
       sum(order_final_amount)               order_final_amount,
       sum(if(order_final_amount > 0, 1, 0)) order_user_count
from (
         select recent_days,
                user_id,
                case
                    when recent_days = 0 then order_count
                    when recent_days = 1 then order_last_1d_count
                    when recent_days = 7 then order_last_7d_count
                    when recent_days = 30 then order_last_30d_count
                    end order_count,
                case
                    when recent_days = 0 then order_final_amount
                    when recent_days = 1 then order_last_1d_final_amount
                    when recent_days = 7 then order_last_7d_final_amount
                    when recent_days = 30 then order_last_30d_final_amount
                    end order_final_amount
         from dwt_user_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
     ) t1
group by recent_days;

-- ************************************************************
-- 各地区订单统计


-- TODO：我是从dwd_order_info表中出的数据，这个里面没有订单为0的数据
--      dwt_area_topic这个里面有订单为0 的数据。

select "2020-06-14",
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       count(t1.id)         order_count,
       sum(t1.final_amount) order_amount
from (
         select recent_days,
                id,
                user_id,
                province_id,
                final_amount
         from dwd_order_info lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt >= date_add("2020-06-14", -recent_days + 1)
     ) t1
         left join (
    select id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_base_province
) t2
                   on t1.province_id = t2.id
group by recent_days, province_id, province_name, area_code, iso_code, iso_3166_2
order by cast(province_id as int), recent_days
;


-- 这个是从dwt_area_topic表中出的数据。根据文档写的
select "2020-06-14",
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count,
       order_amount
from (
         select recent_days,
                province_id,
                case recent_days
                    when 1 then order_last_1d_count
                    when 7 then order_last_7d_count
                    when 30 then order_last_30d_count
                    end order_count,
                case recent_days
                    when 1 then order_last_1d_final_amount
                    when 7 then order_last_7d_final_amount
                    when 30 then order_last_30d_final_amount
                    end order_amount
         from dwt_area_topic lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = "2020-06-14"
     ) t1
         left join (
    select id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2
    from dim_base_province
) t2
                   on t1.province_id = t2.id
order by cast(province_id as int), recent_days
;

-- 这个是文档的答案。
-- TODO：感觉文档的答案比较复杂，文档把recent_days和province_id分组的了，然后做的sum()操作，
--  可是dwt_area_topic就是各地区订单统计表，表里面总共就34个省份且recent_days也都不一样，就没必要做分组操作了
select dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count,
       order_amount
from (
         select '2020-06-14'      dt,
                recent_days,
                province_id,
                sum(order_count)  order_count,
                sum(order_amount) order_amount
         from (
                  select recent_days,
                         province_id,
                         case
                             when recent_days = 1 then order_last_1d_count
                             when recent_days = 7 then order_last_7d_count
                             when recent_days = 30 then order_last_30d_count
                             end order_count,
                         case
                             when recent_days = 1 then order_last_1d_final_amount
                             when recent_days = 7 then order_last_7d_final_amount
                             when recent_days = 30 then order_last_30d_final_amount
                             end order_amount
                  from dwt_area_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, province_id
     ) t2
         join dim_base_province t3
              on t2.province_id = t3.id
order by cast(province_id as int), recent_days
;

-- ***************************************************************
-- 优惠券统计
select "2020-06-14",
       coupon_id,
       coupon_name,
       date_format(start_time, 'yyyy-MM-dd') start_date,
       coupon_type                           rule_name,
       get_count,
       order_count,
       expire_count,
       order_original_amount,
       reduce_amount,
       reduce_rate
from (
         select coupon_id,
                get_count,
                order_count,
                order_original_amount,
                order_final_amount,
                order_reduce_amount                                                 reduce_amount,
                expire_count,
                cast(order_reduce_amount / order_original_amount as decimal(16, 2)) reduce_rate
         from dwt_coupon_topic
         where dt = "2020-06-14"
     ) t1
         left join (
    select id,
           coupon_name,
           start_time,
           coupon_type
    from dim_coupon_info
    where dt = "2020-06-14"
) t2
                   on t1.coupon_id = t2.id
;

-- ***************************************************************
-- 活动统计

select "2020-06-14",
       t1.activity_id,
       activity_name,
       date_format(start_time, 'yyyy-MM-dd') start_date,
       order_count,
       order_original_amount,
       order_final_amount,
       order_reduce_amount                   reduce_amount,
       reduce_rate
from (
         select activity_id,
                sum(order_count)                                                                    order_count,
                sum(order_original_amount)                                                          order_original_amount,
                sum(order_final_amount)                                                             order_final_amount,
                sum(order_reduce_amount)                                                            order_reduce_amount,
                cast(sum(order_reduce_amount) / sum(order_original_amount) * 100 as decimal(16, 2)) reduce_rate
         from dwt_activity_topic
         where dt = "2020-06-14"
         group by activity_id
     ) t1
         left join (
    select activity_id,
           activity_name,
           start_time
    from dim_activity_rule_info
    where dt = "2020-06-14"
) t2
                   on t1.activity_id = t2.activity_id
;


