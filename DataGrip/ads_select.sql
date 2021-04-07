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

-- 每月品牌复购率 = 某品牌本月被购买的次数 / 所有品牌本月被购买的次数。

from dwt_sku_topic lateral view explode(array(1, 7, 30)) tmp as recent_days
select sku_id,
       recent_days
;

-- 1天的
select sku_id,
        order_last_1d_count,
       sum(order_last_1d_count)
from dwt_sku_topic
where dt="2020-06-14"
;

select sku_id,
       sum(order_last_1d_count)
from dwt_sku_topic
where dt="2020-06-14"
;
-- ************************************************************
