use gmall;


select brand,
       count(*) over ()
from dwd_page_log
where dt = "2020-06-14"
group by brand;

select user_id,
       login_date_first
from dwt_user_topic
where dt = "2020-06-14";

from (
         select user_id,
                login_date_first,
                login_date_last
         from dwt_user_topic
         where dt = "2020-06-14"
     ) t1
select "2020-06-14",
--        login_date_first,
       sum(if(datediff(login_date_last, login_date_first) == 0, 1, 0))
--        count(if(datediff(login_date_last, login_date_first) = 0), 1, 0) retention_count
;


select '2020-06-14',
       login_date_first                                                                       create_date,
       datediff('2020-06-14', login_date_first)                                               retention_day,
       sum(if(login_date_last = '2020-06-14', 1, 0))                                          retention_count,
       count(*)                                                                               new_user_count,
       cast(sum(if(login_date_last = '2020-06-14', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from dwt_user_topic
where dt = '2020-06-14'
  and login_date_first >= date_add('2020-06-14', -7)
  and login_date_first < '2020-06-14'
group by login_date_first
;

select login_date_first,
       login_date_last
from dwt_user_topic
where dt = "2020-06-14"
  and login_date_first >= date_add("2020-06-14", -3);

desc function extended over
;

SHOW FUNCTIONS;


from dwt_user_topic
select user_id,
       count(*) over()
;


from dwt_user_topic
select user_id,
       login_date_1d_count,
       count(*) over(partition by login_date_1d_count)
;


from dwt_user_topic
select user_id,
       login_date_1d_count,
       count(*) over(order by login_date_1d_count)
;

from dwt_user_topic
select user_id,
       login_date_1d_count,
       count(*) over(order by login_date_1d_count)
;

from dwt_user_topic
select user_id,
       login_date_1d_count,
       count(*) over(partition by login_last_1d_day_count order by login_date_1d_count)
;

desc function extended explode;
desc function extended split;

SELECT   explode(array(10, 20));