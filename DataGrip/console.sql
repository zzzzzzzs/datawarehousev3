create database gmall;
use gmall;

drop table if exists ods_log;
-- 创建表
CREATE EXTERNAL TABLE ods_log (`line` string)
PARTITIONED BY (`dt` string) -- 按照时间创建分区
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_log'  -- 指定数据在hdfs上的存储位置
;

-- 加载数据
load data inpath '/origin_data/gmall/log/topic_log/2020-06-14' into table ods_log partition(dt='2020-06-14');
-- 为lzo建立索引
-- hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer -Dmapreduce.job.queuename=hive /warehouse/gmall/ods/ods_log/dt=2020-06-14



-- 3.3.1 订单表（增量及更新）
-- hive (gmall)>
drop table if exists ods_order_info;
create external table ods_order_info (
    `id` string COMMENT '订单号',
    `final_total_amount` decimal(16,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `province_id` string COMMENT '省份ID',
    `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
    `original_total_amount` decimal(16,2)  COMMENT '原价金额',
    `feight_fee` decimal(16,2)  COMMENT '运费'
) COMMENT '订单表'
PARTITIONED BY (`dt` string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；输出数据采用TextOutputFormat
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_info/' -- 指定数据在hdfs上的存储位置
;
-- 3.3.2 订单详情表（增量）
-- hive (gmall)>
drop table if exists ods_order_detail;
create external table ods_order_detail(
    `id` string COMMENT '编号',
    `order_id` string  COMMENT '订单号',
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT '商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` decimal(16,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间',
    `source_type` string COMMENT '来源类型',
    `source_id` string COMMENT '来源编号'
) COMMENT '订单详情表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_detail/';
-- 3.3.3 SKU商品表（全量）
-- hive (gmall)>
drop table if exists ods_sku_info;
create external table ods_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id` string   COMMENT 'spuid',
    `price` decimal(16,2) COMMENT '价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` string COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `create_time` string COMMENT '创建时间'
) COMMENT 'SKU商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_sku_info/';
-- 3.3.4 用户表（增量及更新）
-- hive (gmall)>
drop table if exists ods_user_info;
create external table ods_user_info(
    `id` string COMMENT '用户id',
    `name`  string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT '用户表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_user_info/';
-- 3.3.5 商品一级分类表（全量）
-- hive (gmall)>
drop table if exists ods_base_category1;
create external table ods_base_category1(
    `id` string COMMENT 'id',
    `name`  string COMMENT '名称'
) COMMENT '商品一级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category1/';
-- 3.3.6 商品二级分类表（全量）
-- hive (gmall)>
drop table if exists ods_base_category2;
create external table ods_base_category2(
    `id` string COMMENT ' id',
    `name` string COMMENT '名称',
    category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category2/';
-- 3.3.7 商品三级分类表（全量）
-- hive (gmall)>
drop table if exists ods_base_category3;
create external table ods_base_category3(
    `id` string COMMENT ' id',
    `name`  string COMMENT '名称',
    category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category3/';
-- 3.3.8 支付流水表（增量）
-- hive (gmall)>
drop table if exists ods_payment_info;
create external table ods_payment_info(
    `id`   bigint COMMENT '编号',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `total_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type`    string COMMENT '支付类型',
    `payment_time`    string COMMENT '支付时间'
)  COMMENT '支付流水表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_payment_info/';
-- 3.3.9 省份表（特殊）
-- hive (gmall)>
drop table if exists ods_base_province;
create external table ods_base_province (
    `id`   bigint COMMENT '编号',
    `name`        string COMMENT '省份名称',
    `region_id`    string COMMENT '地区ID',
    `area_code`    string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码,superset可视化使用'
)  COMMENT '省份表'
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_province/';
-- 3.3.10 地区表（特殊）
-- hive (gmall)>
drop table if exists ods_base_region;
create external table ods_base_region (
    `id` string COMMENT '编号',
    `region_name` string COMMENT '地区名称'
)  COMMENT '地区表'
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_region/';
-- 3.3.11 品牌表（全量）
-- hive (gmall)>
drop table if exists ods_base_trademark;
create external table ods_base_trademark (
    `tm_id`   string COMMENT '编号',
    `tm_name` string COMMENT '品牌名称'
)  COMMENT '品牌表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_trademark/';
-- 3.3.12 订单状态表（增量）
-- hive (gmall)>
drop table if exists ods_order_status_log;
create external table ods_order_status_log (
    `id`   string COMMENT '编号',
    `order_id` string COMMENT '订单ID',
    `order_status` string COMMENT '订单状态',
    `operate_time` string COMMENT '修改时间'
)  COMMENT '订单状态表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_status_log/';
-- 3.3.13 SPU商品表（全量）
-- hive (gmall)>
drop table if exists ods_spu_info;
create external table ods_spu_info(
    `id` string COMMENT 'spuid',
    `spu_name` string COMMENT 'spu名称',
    `category3_id` string COMMENT '品类id',
    `tm_id` string COMMENT '品牌id'
) COMMENT 'SPU商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_spu_info/';
-- 3.3.14 商品评论表（增量）
-- hive (gmall)>
drop table if exists ods_comment_info;
create external table ods_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_comment_info/';
-- 3.3.15 退单表（增量）
-- hive (gmall)>
drop table if exists ods_order_refund_info;
create external table ods_order_refund_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` bigint COMMENT '退款件数',
    `refund_amount` decimal(16,2) COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退单表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_refund_info/';
-- 3.3.16 加购表（全量）
-- hive (gmall)>
drop table if exists ods_cart_info;
create external table ods_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` decimal(16,2)  COMMENT '放入购物车时价格',
    `sku_num` bigint  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单',
    `order_time` string  COMMENT '下单时间',
    `source_type` string COMMENT '来源类型',
    `source_id` string COMMENT '来源编号'
) COMMENT '加购表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_cart_info/';
-- 3.3.17 商品收藏表（全量）
-- hive (gmall)>
drop table if exists ods_favor_info;
create external table ods_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '商品收藏表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_favor_info/';
-- 3.3.18 优惠券领用表（新增及变化）
-- hive (gmall)>
drop table if exists ods_coupon_use;
create external table ods_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'skuid',
    `order_id` string  COMMENT 'spuid',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_use/';
-- 3.3.19 优惠券表（全量）
-- hive (gmall)>
drop table if exists ods_coupon_info;
create external table ods_coupon_info(
  `id` string COMMENT '购物券编号',
  `coupon_name` string COMMENT '购物券名称',
  `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
  `condition_amount` decimal(16,2) COMMENT '满额数',
  `condition_num` bigint COMMENT '满件数',
  `activity_id` string COMMENT '活动编号',
  `benefit_amount` decimal(16,2) COMMENT '减金额',
  `benefit_discount` decimal(16,2) COMMENT '折扣',
  `create_time` string COMMENT '创建时间',
  `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
  `spu_id` string COMMENT '商品id',
  `tm_id` string COMMENT '品牌id',
  `category3_id` string COMMENT '品类id',
  `limit_num` bigint COMMENT '最多领用次数',
  `operate_time`  string COMMENT '修改时间',
  `expire_time`  string COMMENT '过期时间'
) COMMENT '优惠券表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_info/';
-- 3.3.20 活动表（全量）
-- hive (gmall)>
drop table if exists ods_activity_info;
create external table ods_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间'
) COMMENT '活动表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_info/';
-- 3.3.21 活动订单关联表（增量）
-- hive (gmall)>
drop table if exists ods_activity_order;
create external table ods_activity_order(
    `id` string COMMENT '编号',
    `activity_id` string  COMMENT '优惠券ID',
    `order_id` string  COMMENT 'skuid',
    `create_time` string  COMMENT '领取时间'
) COMMENT '活动订单关联表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_order/';
-- 3.3.22 优惠规则表（全量）
-- hive (gmall)>
drop table if exists ods_activity_rule;
create external table ods_activity_rule(
    `id` string COMMENT '编号',
    `activity_id` string  COMMENT '活动ID',
    `condition_amount` decimal(16,2) COMMENT '满减金额',
    `condition_num` bigint COMMENT '满减件数',
    `benefit_amount` decimal(16,2) COMMENT '优惠金额',
    `benefit_discount` decimal(16,2) COMMENT '优惠折扣',
    `benefit_level` string  COMMENT '优惠级别'
) COMMENT '优惠规则表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_rule/';
-- 3.3.23 编码字典表（全量）
-- hive (gmall)>
drop table if exists ods_base_dic;
create external table ods_base_dic(
    `dic_code` string COMMENT '编号',
    `dic_name` string  COMMENT '编码名称',
    `parent_code` string  COMMENT '父编码',
    `create_time` string  COMMENT '创建日期',
    `operate_time` string  COMMENT '操作日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_dic/';


select get_json_object('[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]','$[0]');


SELECT get_json_object('[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]',"$[0].age");


-- 创建启动日志表
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `entry` string COMMENT ' icon手机图标  notice 通知   install 安装后启动',
    `loading_time` bigint COMMENT '启动加载时间',
    `open_ad_id` string COMMENT '广告页ID ',
    `open_ad_ms` bigint COMMENT '广告总共播放时间',
    `open_ad_skip_ms` bigint COMMENT '用户跳过广告时点',
    `ts` bigint COMMENT '时间'
) COMMENT '启动日志表'
PARTITIONED BY (dt string) -- 按照时间创建分区
stored as parquet -- 采用parquet列式存储
LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
TBLPROPERTIES('parquet.compression'='lzo') -- 采用LZO压缩
;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;



insert into table dwd_start_log partition (dt='2020-06-14')
select
*
from ods_log
where dt='2020-06-14'
and get_json_object(line, '$.start') is not null;



insert overwrite table dwd_start_log partition (dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.start') is not null;

select * from dwd_start_log where dt='2020-06-14' limit 2;





drop table if exists dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `during_time` bigint COMMENT '持续时间毫秒',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `ts` bigint
) COMMENT '页面日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_page_log'
TBLPROPERTIES('parquet.compression'='lzo');

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_page_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.page') is not null;

select * from dwd_page_log where dt='2020-06-14' limit 50;


drop table if exists dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `during_time` bigint COMMENT '持续时间毫秒',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面id ',
    `source_type` string COMMENT '来源类型',
    `action_id` string COMMENT '动作id',
    `item` string COMMENT '目标id ',
    `item_type` string COMMENT '目标类型',
    `ts` bigint COMMENT '时间'
) COMMENT '动作日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_action_log'
TBLPROPERTIES('parquet.compression'='lzo');



create function explode_json_array as 'com.me.hive.udtf.ExplodeJSONArray' using jar 'hdfs://bigdata102:9820/user/hive/jars/hive-function-1.0-SNAPSHOT.jar';

show functions like "*explode*";

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_action_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    get_json_object(action,'$.action_id'),
    get_json_object(action,'$.item'),
    get_json_object(action,'$.item_type'),
    get_json_object(action,'$.ts')
from ods_log lateral view explode_json_array(get_json_object(line,'$.actions')) tmp as action
where dt='2020-06-14'
and get_json_object(line,'$.actions') is not null;

select * from dwd_action_log where dt='2020-06-14' limit 2;


drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `during_time` bigint COMMENT 'app版本号',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `ts` bigint COMMENT 'app版本号',
    `display_type` string COMMENT '曝光类型',
    `item` string COMMENT '曝光对象id ',
    `item_type` string COMMENT 'app版本号',
    `order` bigint COMMENT '出现顺序'
) COMMENT '曝光日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_display_log'
TBLPROPERTIES('parquet.compression'='lzo');

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_display_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    get_json_object(line,'$.ts'),
    get_json_object(display,'$.displayType'),
    get_json_object(display,'$.item'),
    get_json_object(display,'$.item_type'),
    get_json_object(display,'$.order')
from ods_log lateral view explode_json_array(get_json_object(line,'$.displays')) tmp as display
where dt='2020-06-14'
and get_json_object(line,'$.displays') is not null;


select * from dwd_display_log where dt='2020-06-14' limit 2;

drop table if exists dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌',
    `channel` string COMMENT '渠道',
    `model` string COMMENT '手机型号',
    `mid_id` string COMMENT '设备id',
    `os` string COMMENT '操作系统',
    `user_id` string COMMENT '会员id',
    `version_code` string COMMENT 'app版本号',
    `page_item` string COMMENT '目标id ',
    `page_item_type` string COMMENT '目标类型',
    `last_page_id` string COMMENT '上页类型',
    `page_id` string COMMENT '页面ID ',
    `source_type` string COMMENT '来源类型',
    `entry` string COMMENT ' icon手机图标  notice 通知 install 安装后启动',
    `loading_time` string COMMENT '启动加载时间',
    `open_ad_id` string COMMENT '广告页ID ',
    `open_ad_ms` string COMMENT '广告总共播放时间',
    `open_ad_skip_ms` string COMMENT '用户跳过广告时点',
    `actions` string COMMENT '动作',
    `displays` string COMMENT '曝光',
    `ts` string COMMENT '时间',
    `error_code` string COMMENT '错误码',
    `msg` string COMMENT '错误信息'
) COMMENT '错误日志表'
PARTITIONED BY (dt string)
stored as parquet
LOCATION '/warehouse/gmall/dwd/dwd_error_log'
TBLPROPERTIES('parquet.compression'='lzo');


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_error_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.actions'),
    get_json_object(line,'$.displays'),
    get_json_object(line,'$.ts'),
    get_json_object(line,'$.err.error_code'),
    get_json_object(line,'$.err.msg')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.err') is not null;

select * from dwd_error_log where dt='2020-06-14' limit 2;

DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
    `id` string COMMENT '商品id',
    `spu_id` string COMMENT 'spuid',
    `price` decimal(16,2) COMMENT '商品价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` decimal(16,2) COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `tm_name` string COMMENT '品牌名称',
    `category3_id` string COMMENT '三级分类id',
    `category2_id` string COMMENT '二级分类id',
    `category1_id` string COMMENT '一级分类id',
    `category3_name` string COMMENT '三级分类名称',
    `category2_name` string COMMENT '二级分类名称',
    `category1_name` string COMMENT '一级分类名称',
    `spu_name` string COMMENT 'spu名称',
    `create_time` string COMMENT '创建时间'
) COMMENT '商品维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
tblproperties ("parquet.compression"="lzo");



SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_sku_info partition(dt='2020-06-14')
select
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    ob.tm_name,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    spu.spu_name,
    sku.create_time
from
(
    select * from ods_sku_info where dt='2020-06-14'
)sku
join
(
    select * from ods_base_trademark where dt='2020-06-14'
)ob on sku.tm_id=ob.tm_id
join
(
    select * from ods_spu_info where dt='2020-06-14'
)spu on spu.id = sku.spu_id
join
(
    select * from ods_base_category3 where dt='2020-06-14'
)c3 on sku.category3_id=c3.id
join
(
    select * from ods_base_category2 where dt='2020-06-14'
)c2 on c3.category2_id=c2.id
join
(
    select * from ods_base_category1 where dt='2020-06-14'
)c1 on c2.category1_id=c1.id;

select * from dwd_dim_sku_info where dt='2020-06-14' limit 2;

drop table if exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info(
    `id` string COMMENT '购物券编号',
    `coupon_name` string COMMENT '购物券名称',
    `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(16,2) COMMENT '满额数',
    `condition_num` bigint COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` decimal(16,2) COMMENT '减金额',
    `benefit_discount` decimal(16,2) COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` bigint COMMENT '最多领用次数',
    `operate_time`  string COMMENT '修改时间',
    `expire_time`  string COMMENT '过期时间'
) COMMENT '优惠券维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_coupon_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_coupon_info partition(dt='2020-06-14')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    spu_id,
    tm_id,
    category3_id,
    limit_num,
    operate_time,
    expire_time
from ods_coupon_info
where dt='2020-06-14';

select * from dwd_dim_coupon_info where dt='2020-06-14' limit 2;

drop table if exists dwd_dim_activity_info;
create external table dwd_dim_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_activity_info partition(dt='2020-06-14')
select
    id,
    activity_name,
    activity_type,
    start_time,
    end_time,
    create_time
from ods_activity_info
where dt='2020-06-14';

select * from dwd_dim_activity_info where dt='2020-06-14' limit 2;

DROP TABLE IF EXISTS `dwd_dim_base_province`;
CREATE EXTERNAL TABLE `dwd_dim_base_province` (
    `id` string COMMENT 'id',
    `province_name` string COMMENT '省市名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'ISO编码',
    `region_id` string COMMENT '地区id',
    `region_name` string COMMENT '地区名称'
) COMMENT '地区维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_base_province/'
tblproperties ("parquet.compression"="lzo");


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_base_province
select
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.region_id,
    br.region_name
from
(
    select * from ods_base_province
) bp
join
(
    select * from ods_base_region
) br
on bp.region_id = br.id;

select * from dwd_dim_base_province limit 2;

-- 时间维度表（特殊）
DROP TABLE IF EXISTS `dwd_dim_date_info`;
CREATE EXTERNAL TABLE `dwd_dim_date_info`(
    `date_id` string COMMENT '日',
    `week_id` string COMMENT '周',
    `week_day` string COMMENT '周的第几天',
    `day` string COMMENT '每月的第几天',
    `month` string COMMENT '第几月',
    `quarter` string COMMENT '第几季度',
    `year` string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_date_info/'
tblproperties ("parquet.compression"="lzo");


DROP TABLE IF EXISTS `dwd_dim_date_info_tmp`;
CREATE EXTERNAL TABLE `dwd_dim_date_info_tmp`(
    `date_id` string COMMENT '日',
    `week_id` string COMMENT '周',
    `week_day` string COMMENT '周的第几天',
    `day` string COMMENT '每月的第几天',
    `month` string COMMENT '第几月',
    `quarter` string COMMENT '第几季度',
    `year` string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间临时表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_date_info_tmp/';

load data local inpath '/opt/module/db_log/date_info.txt' into table dwd_dim_date_info_tmp;

insert overwrite table dwd_dim_date_info select * from dwd_dim_date_info_tmp;

select * from dwd_dim_date_info;

drop table if exists dwd_fact_payment_info;
create external table dwd_fact_payment_info (
    `id` string COMMENT 'id',
    `out_trade_no` string COMMENT '对外业务编号',
    `order_id` string COMMENT '订单编号',
    `user_id` string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `payment_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type` string COMMENT '支付类型',
    `payment_time` string COMMENT '支付时间',
    `province_id` string COMMENT '省份ID'
) COMMENT '支付事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_payment_info partition(dt='2020-06-14')
select
    pi.id,
    pi.out_trade_no,
    pi.order_id,
    pi.user_id,
    pi.alipay_trade_no,
    pi.total_amount,
    pi.subject,
    pi.payment_type,
    pi.payment_time,
    oi.province_id
from
(
    select * from ods_payment_info where dt='2020-06-14'
)pi
join
(
    select id, province_id from ods_order_info where dt='2020-06-14'
)oi
on pi.order_id = oi.id;

select * from dwd_fact_payment_info where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_order_refund_info;
create external table dwd_fact_order_refund_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` bigint COMMENT '退款件数',
    `refund_amount` decimal(16,2) COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退款事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_refund_info partition(dt='2020-06-14')
select
    id,
    user_id,
    order_id,
    sku_id,
    refund_type,
    refund_num,
    refund_amount,
    refund_reason_type,
    create_time
from ods_order_refund_info
where dt='2020-06-14';

select * from dwd_fact_order_refund_info where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_comment_info;
create external table dwd_fact_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_comment_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_comment_info partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time
from ods_comment_info
where dt='2020-06-14';

select * from dwd_fact_comment_info where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_order_detail;
create external table dwd_fact_order_detail (
    `id` string COMMENT '订单编号',
    `order_id` string COMMENT '订单号',
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT 'sku商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` decimal(16,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间',
    `province_id` string COMMENT '省份ID',
    `source_type` string COMMENT '来源类型',
    `source_id` string COMMENT '来源编号',
    `original_amount_d` decimal(20,2) COMMENT '原始价格分摊',
    `final_amount_d` decimal(20,2) COMMENT '购买价格分摊',
    `feight_fee_d` decimal(20,2) COMMENT '分摊运费',
    `benefit_reduce_amount_d` decimal(20,2) COMMENT '分摊优惠'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_detail partition(dt='2020-06-14')
select
    id,
    order_id,
    user_id,
    sku_id,
    sku_name,
    order_price,
    sku_num,
    create_time,
    province_id,
    source_type,
    source_id,
    original_amount_d,
    if(rn=1,final_total_amount -(sum_div_final_amount - final_amount_d),final_amount_d),
    if(rn=1,feight_fee - (sum_div_feight_fee - feight_fee_d),feight_fee_d),
    if(rn=1,benefit_reduce_amount - (sum_div_benefit_reduce_amount -benefit_reduce_amount_d), benefit_reduce_amount_d)
from
(
    select
        od.id,
        od.order_id,
        od.user_id,
        od.sku_id,
        od.sku_name,
        od.order_price,
        od.sku_num,
        od.create_time,
        oi.province_id,
        od.source_type,
        od.source_id,
        round(od.order_price*od.sku_num,2) original_amount_d,
        round(od.order_price*od.sku_num/oi.original_total_amount*oi.final_total_amount,2) final_amount_d,
        round(od.order_price*od.sku_num/oi.original_total_amount*oi.feight_fee,2) feight_fee_d,
        round(od.order_price*od.sku_num/oi.original_total_amount*oi.benefit_reduce_amount,2) benefit_reduce_amount_d,
        row_number() over(partition by od.order_id order by od.id desc) rn,
        oi.final_total_amount,
        oi.feight_fee,
        oi.benefit_reduce_amount,
        sum(round(od.order_price*od.sku_num/oi.original_total_amount*oi.final_total_amount,2)) over(partition by od.order_id) sum_div_final_amount,
        sum(round(od.order_price*od.sku_num/oi.original_total_amount*oi.feight_fee,2)) over(partition by od.order_id) sum_div_feight_fee,
        sum(round(od.order_price*od.sku_num/oi.original_total_amount*oi.benefit_reduce_amount,2)) over(partition by od.order_id) sum_div_benefit_reduce_amount
    from
    (
        select * from ods_order_detail where dt='2020-06-14'
    ) od
    join
    (
        select * from ods_order_info where dt='2020-06-14'
    ) oi
    on od.order_id=oi.id
)t1;

select * from dwd_fact_order_detail where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_cart_info;
create external table dwd_fact_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` string  COMMENT '放入购物车时价格',
    `sku_num` string  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单。1为已下单;0为未下单',
    `order_time` string  COMMENT '下单时间',
    `source_type` string COMMENT '来源类型',
    `srouce_id` string COMMENT '来源编号'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_cart_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_cart_info partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    cart_price,
    sku_num,
    sku_name,
    create_time,
    operate_time,
    is_ordered,
    order_time,
    source_type,
    source_id
from ods_cart_info
where dt='2020-06-14';

select * from dwd_fact_cart_info where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_favor_info;
create external table dwd_fact_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_favor_info/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_favor_info partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ods_favor_info
where dt='2020-06-14';

select * from dwd_fact_favor_info where dt='2020-06-14' limit 2;

drop table if exists dwd_fact_coupon_use;
create external table dwd_fact_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'userid',
    `order_id` string  COMMENT '订单id',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_coupon_use/'
tblproperties ("parquet.compression"="lzo");

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_coupon_use partition(dt)
select
    if(new.id is null,old.id,new.id),
    if(new.coupon_id is null,old.coupon_id,new.coupon_id),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.order_id is null,old.order_id,new.order_id),
    if(new.coupon_status is null,old.coupon_status,new.coupon_status),
    if(new.get_time is null,old.get_time,new.get_time),
    if(new.using_time is null,old.using_time,new.using_time),
    if(new.used_time is null,old.used_time,new.used_time),
    date_format(if(new.get_time is null,old.get_time,new.get_time),'yyyy-MM-dd')
from
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from dwd_fact_coupon_use
    where dt in
    (
        select
            date_format(get_time,'yyyy-MM-dd')
        from ods_coupon_use
        where dt='2020-06-14'
    )
)old
full outer join
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from ods_coupon_use
    where dt='2020-06-14'
)new
on old.id=new.id;

select * from dwd_fact_coupon_use where dt='2020-06-14' limit 2;

drop table if exists stud;
create table stud (name string, area string, course string, score int);

drop table if exists dwd_fact_order_info;
create external table dwd_fact_order_info (
    `id` string COMMENT '订单编号',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间(未支付状态)',
    `payment_time` string COMMENT '支付时间(已支付状态)',
    `cancel_time` string COMMENT '取消时间(已取消状态)',
    `finish_time` string COMMENT '完成时间(已完成状态)',
    `refund_time` string COMMENT '退款时间(退款中状态)',
    `refund_finish_time` string COMMENT '退款完成时间(退款完成状态)',
    `province_id` string COMMENT '省份ID',
    `activity_id` string COMMENT '活动ID',
    `original_total_amount` decimal(16,2) COMMENT '原价金额',
    `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
    `feight_fee` decimal(16,2) COMMENT '运费',
    `final_total_amount` decimal(16,2) COMMENT '订单金额'
) COMMENT '订单事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_info/'
tblproperties ("parquet.compression"="lzo");


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_fact_order_info partition(dt)
select
    if(new.id is null,old.id,new.id),
    if(new.order_status is null,old.order_status,new.order_status),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.out_trade_no is null,old.out_trade_no,new.out_trade_no),
    if(new.tms['1001'] is null,old.create_time,new.tms['1001']),--1001对应未支付状态
    if(new.tms['1002'] is null,old.payment_time,new.tms['1002']),
    if(new.tms['1003'] is null,old.cancel_time,new.tms['1003']),
    if(new.tms['1004'] is null,old.finish_time,new.tms['1004']),
    if(new.tms['1005'] is null,old.refund_time,new.tms['1005']),
    if(new.tms['1006'] is null,old.refund_finish_time,new.tms['1006']),
    if(new.province_id is null,old.province_id,new.province_id),
    if(new.activity_id is null,old.activity_id,new.activity_id),
    if(new.original_total_amount is null,old.original_total_amount,new.original_total_amount),
    if(new.benefit_reduce_amount is null,old.benefit_reduce_amount,new.benefit_reduce_amount),
    if(new.feight_fee is null,old.feight_fee,new.feight_fee),
    if(new.final_total_amount is null,old.final_total_amount,new.final_total_amount),
    date_format(if(new.tms['1001'] is null,old.create_time,new.tms['1001']),'yyyy-MM-dd')
from
(
    select
        id,
        order_status,
        user_id,
        out_trade_no,
        create_time,
        payment_time,
        cancel_time,
        finish_time,
        refund_time,
        refund_finish_time,
        province_id,
        activity_id,
        original_total_amount,
        benefit_reduce_amount,
        feight_fee,
        final_total_amount
    from dwd_fact_order_info
    where dt
    in
    (
        select
          date_format(create_time,'yyyy-MM-dd')
        from ods_order_info
        where dt='2020-06-14'
    )
)old
full outer join
(
    select
        info.id,
        info.order_status,
        info.user_id,
        info.out_trade_no,
        info.province_id,
        act.activity_id,
        log.tms,
        info.original_total_amount,
        info.benefit_reduce_amount,
        info.feight_fee,
        info.final_total_amount
    from
    (
        select
            order_id,
            str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') tms
        from ods_order_status_log
        where dt='2020-06-14'
        group by order_id
    )log
    join
    (
        select * from ods_order_info where dt='2020-06-14'
    )info
    on log.order_id=info.id
    left join
    (
        select * from ods_activity_order where dt='2020-06-14'
    )act
    on log.order_id=act.order_id
)new
on old.id=new.id;

select * from dwd_fact_order_info where dt='2020-06-14' limit 2;

drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_user_info_his
select
    id,
    name,
    birthday,
    gender,
    email,
    user_level,
    create_time,
    operate_time,
    '2020-06-14',
    '9999-99-99'
from ods_user_info oi
where oi.dt='2020-06-14';


drop table if exists dwd_dim_user_info_his_tmp;
create external table dwd_dim_user_info_his_tmp(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '订单拉链临时表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his_tmp/'
tblproperties ("parquet.compression"="lzo");

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_dim_user_info_his_tmp
select * from
(
    select
        id,
        name,
        birthday,
        gender,
        email,
        user_level,
        create_time,
        operate_time,
        '2020-06-14' start_date,
        '9999-99-99' end_date
    from ods_user_info where dt='2020-06-14'

    union all
    select
        uh.id,
        uh.name,
        uh.birthday,
        uh.gender,
        uh.email,
        uh.user_level,
        uh.create_time,
        uh.operate_time,
        uh.start_date,
        if(ui.id is not null  and uh.end_date='9999-99-99', date_add(ui.dt,-1), uh.end_date) end_date
    from dwd_dim_user_info_his uh left join
    (
        select
            *
        from ods_user_info
        where dt='2020-06-14'
    ) ui on uh.id=ui.id
)his
order by his.id, start_date;

insert overwrite table dwd_dim_user_info_his
select * from dwd_dim_user_info_his_tmp;

select id, start_date, end_date from dwd_dim_user_info_his limit 2;

drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
    `mid_id`      string COMMENT '设备id',
    `brand`       string COMMENT '手机品牌',
    `model`       string COMMENT '手机型号',
    `login_count` bigint COMMENT '活跃次数',
    `page_stats`  array<struct<page_id:string,page_count:bigint>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount'
tblproperties ("parquet.compression"="lzo");


with
tmp_start as
(
    select
        mid_id,
        brand,
        model,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    group by mid_id,brand,model
),
tmp_page as
(
    select
        mid_id,
        brand,
        model,        collect_set(named_struct('page_id',page_id,'page_count',page_count)) page_stats
    from
    (
        select
            mid_id,
            brand,
            model,
            page_id,
            count(*) page_count
        from dwd_page_log
        where dt='2020-06-14'
        group by mid_id,brand,model,page_id
    )tmp
    group by mid_id,brand,model
)
insert overwrite table dws_uv_detail_daycount partition(dt='2020-06-14')
select
    nvl(tmp_start.mid_id,tmp_page.mid_id),
    nvl(tmp_start.brand,tmp_page.brand),
    nvl(tmp_start.model,tmp_page.model),
    tmp_start.login_count,
    tmp_page.page_stats
from tmp_start
full outer join tmp_page
on tmp_start.mid_id=tmp_page.mid_id
and tmp_start.brand=tmp_page.brand
and tmp_start.model=tmp_page.model;

select * from dws_uv_detail_daycount where dt='2020-06-14' limit 2;

drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount
(
    user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    cart_count bigint comment '加入购物车次数',
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额',
    order_detail_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='cart_add'
    group by user_id
),tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from dwd_fact_order_info
    where dt='2020-06-14'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_fact_payment_info
    where dt='2020-06-14'
    group by user_id
),
tmp_order_detail as
(
    select
        user_id,
        collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'order_amount',order_amount)) order_stats
    from
    (
        select
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(final_amount_d) as decimal(20,2)) order_amount
        from dwd_fact_order_detail
        where dt='2020-06-14'
        group by user_id,sku_id
    )tmp
    group by user_id
)

insert overwrite table dws_user_action_daycount partition(dt='2020-06-14')
select
    tmp_login.user_id,
    login_count,
    nvl(cart_count,0),
    nvl(order_count,0),
    nvl(order_amount,0.0),
    nvl(payment_count,0),
    nvl(payment_amount,0.0),
    order_stats
from tmp_login
left join tmp_cart on tmp_login.user_id=tmp_cart.user_id
left join tmp_order on tmp_login.user_id=tmp_order.user_id
left join tmp_payment on tmp_login.user_id=tmp_payment.user_id
left join tmp_order_detail on tmp_login.user_id=tmp_order_detail.user_id;

select * from dws_user_action_daycount where dt='2020-06-14' limit 2;


drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount
(
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_order as
(
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(final_amount_d) order_amount
    from dwd_fact_order_detail
    where dt='2020-06-14'
    group by sku_id
),
tmp_payment as
(
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(final_amount_d) payment_amount
    from dwd_fact_order_detail
    where (dt='2020-06-14'
    or dt=date_add('2020-06-14',-1))
    and order_id in
    (
        select
            id
        from dwd_fact_order_info
        where (dt='2020-06-14'
        or dt=date_add('2020-06-14',-1))
        and date_format(payment_time,'yyyy-MM-dd')='2020-06-14'
    )
    group by sku_id
),
tmp_refund as
(
    select
        sku_id,
        count(*) refund_count,
        sum(refund_num) refund_num,
        sum(refund_amount) refund_amount
    from dwd_fact_order_refund_info
    where dt='2020-06-14'
    group by sku_id
),
tmp_cart as
(
    select
        item sku_id,
        count(*) cart_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='cart_add'
    group by item
),tmp_favor as
(
    select
        item sku_id,
        count(*) favor_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='favor_add'
    group by item
),
tmp_appraise as
(
select
    sku_id,
    sum(if(appraise='1201',1,0)) appraise_good_count,
    sum(if(appraise='1202',1,0)) appraise_mid_count,
    sum(if(appraise='1203',1,0)) appraise_bad_count,
    sum(if(appraise='1204',1,0)) appraise_default_count
from dwd_fact_comment_info
where dt='2020-06-14'
group by sku_id
)

insert overwrite table dws_sku_action_daycount partition(dt='2020-06-14')
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_count),
    sum(refund_num),
    sum(refund_amount),
    sum(cart_count),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count)
from
(
    select
        sku_id,
        order_count,
        order_num,
        order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_payment
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_refund
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cart
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_favor
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_appraise
)tmp
group by sku_id;

select * from dws_sku_action_daycount where dt='2020-06-14' limit 2;

drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_count` bigint COMMENT '曝光次数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_op as
(
    select
        activity_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    and activity_id is not null
    group by activity_id
),
tmp_display as
(
    select
        item activity_id,
        count(*) display_count
    from dwd_display_log
    where dt='2020-06-14'
    and item_type='activity_id'
    group by item
),
tmp_activity as
(
    select
        *
    from dwd_dim_activity_info
    where dt='2020-06-14'
)
insert overwrite table dws_activity_info_daycount partition(dt='2020-06-14')
select
    nvl(tmp_op.activity_id,tmp_display.activity_id),
    tmp_activity.activity_name,
    tmp_activity.activity_type,
    tmp_activity.start_time,
    tmp_activity.end_time,
    tmp_activity.create_time,
    tmp_display.display_count,
    tmp_op.order_count,
    tmp_op.order_amount,
    tmp_op.payment_count,
    tmp_op.payment_amount
from tmp_op
full outer join tmp_display on tmp_op.activity_id=tmp_display.activity_id
left join tmp_activity on nvl(tmp_op.activity_id,tmp_display.activity_id)=tmp_activity.id;

select * from dws_activity_info_daycount where dt='2020-06-14' limit 2;

drop table if exists dws_area_stats_daycount;
create external table dws_area_stats_daycount(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_count` string COMMENT '活跃设备数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_area_stats_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_login as
(
    select
        area_code,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    group by area_code
),
tmp_op as
(
    select
        province_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    group by province_id
)
insert overwrite table dws_area_stats_daycount partition(dt='2020-06-14')
select
    pro.id,
    pro.province_name,
    pro.area_code,
    pro.iso_code,
    pro.region_id,
    pro.region_name,
    nvl(tmp_login.login_count,0),
    nvl(tmp_op.order_count,0),
    nvl(tmp_op.order_amount,0.0),
    nvl(tmp_op.payment_count,0),
    nvl(tmp_op.payment_amount,0.0)
from dwd_dim_base_province pro
left join tmp_login on pro.area_code=tmp_login.area_code
left join tmp_op on pro.id=tmp_op.province_id;

select * from dws_area_stats_daycount where dt='2020-06-14' limit 2;

select * from dws_uv_detail_daycount where dt='2020-06-15' limit 2;
select * from dws_user_action_daycount where dt='2020-06-15' limit 2;
select * from dws_sku_action_daycount where dt='2020-06-15' limit 2;
select * from dws_activity_info_daycount where dt='2020-06-15' limit 2;
select * from dws_area_stats_daycount where dt='2020-06-15' limit 2;

drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
    `mid_id` string comment '设备id',
    `brand` string comment '手机品牌',
    `model` string comment '手机型号',
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_uv_topic
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.model,old.model),
    nvl(new.brand,old.brand),
    if(old.mid_id is null,'2020-06-14',old.login_date_first),
    if(new.mid_id is not null,'2020-06-14',old.login_date_last),
    if(new.mid_id is not null, new.login_count,0),
    nvl(old.login_count,0)+if(new.login_count>0,1,0)
from
(
    select
        *
    from dwt_uv_topic
)old
full outer join
(
    select
        *
    from dws_uv_detail_daycount
    where dt='2020-06-14'
)new
on old.mid_id=new.mid_id;

select * from dwt_uv_topic limit 5;

drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
)COMMENT '会员主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2020-06-14',old.login_date_first),
    if(new.login_count>0,'2020-06-14',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2020-06-14',old.order_date_first),
    if(new.order_count>0,'2020-06-14',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2020-06-14',old.payment_date_first),
    if(new.payment_count>0,'2020-06-14',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
dwt_user_topic old
full outer join
(
    select
        user_id,
        sum(if(dt='2020-06-14',login_count,0)) login_count,
        sum(if(dt='2020-06-14',order_count,0)) order_count,
        sum(if(dt='2020-06-14',order_amount,0)) order_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_count,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_amount,
        sum(if(login_count>0,1,0)) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_user_action_daycount
    where dt>=date_add( '2020-06-14',-30)
    group by user_id
)new
on old.user_id=new.user_id;

select * from dwt_user_topic limit 5;

drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(16,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(16,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_count bigint comment '累积被加入购物车次数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");


insert overwrite table dwt_sku_topic
select
    nvl(new.sku_id,old.sku_id),
    sku_info.spu_id,
    nvl(new.order_count30,0),
    nvl(new.order_num30,0),
    nvl(new.order_amount30,0),
    nvl(old.order_count,0) + nvl(new.order_count,0),
    nvl(old.order_num,0) + nvl(new.order_num,0),
    nvl(old.order_amount,0) + nvl(new.order_amount,0),
    nvl(new.payment_count30,0),
    nvl(new.payment_num30,0),
    nvl(new.payment_amount30,0),
    nvl(old.payment_count,0) + nvl(new.payment_count,0),
    nvl(old.payment_num,0) + nvl(new.payment_count,0),
    nvl(old.payment_amount,0) + nvl(new.payment_count,0),
    nvl(new.refund_count30,0),
    nvl(new.refund_num30,0),
    nvl(new.refund_amount30,0),
    nvl(old.refund_count,0) + nvl(new.refund_count,0),
    nvl(old.refund_num,0) + nvl(new.refund_num,0),
    nvl(old.refund_amount,0) + nvl(new.refund_amount,0),
    nvl(new.cart_count30,0),
    nvl(old.cart_count,0) + nvl(new.cart_count,0),
    nvl(new.favor_count30,0),
    nvl(old.favor_count,0) + nvl(new.favor_count,0),
    nvl(new.appraise_good_count30,0),
    nvl(new.appraise_mid_count30,0),
    nvl(new.appraise_bad_count30,0),
    nvl(new.appraise_default_count30,0)  ,
    nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0),
    nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0),
    nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0)
from
dwt_sku_topic old
full outer join
(
    select
        sku_id,
        sum(if(dt='2020-06-14', order_count,0 )) order_count,
        sum(if(dt='2020-06-14',order_num ,0 ))  order_num,
        sum(if(dt='2020-06-14',order_amount,0 )) order_amount ,
        sum(if(dt='2020-06-14',payment_count,0 )) payment_count,
        sum(if(dt='2020-06-14',payment_num,0 )) payment_num,
        sum(if(dt='2020-06-14',payment_amount,0 )) payment_amount,
        sum(if(dt='2020-06-14',refund_count,0 )) refund_count,
        sum(if(dt='2020-06-14',refund_num,0 )) refund_num,
        sum(if(dt='2020-06-14',refund_amount,0 )) refund_amount,
        sum(if(dt='2020-06-14',cart_count,0 )) cart_count,
        sum(if(dt='2020-06-14',favor_count,0 )) favor_count,
        sum(if(dt='2020-06-14',appraise_good_count,0 )) appraise_good_count,
        sum(if(dt='2020-06-14',appraise_mid_count,0 ) ) appraise_mid_count ,
        sum(if(dt='2020-06-14',appraise_bad_count,0 )) appraise_bad_count,
        sum(if(dt='2020-06-14',appraise_default_count,0 )) appraise_default_count,
        sum(order_count) order_count30 ,
        sum(order_num) order_num30,
        sum(order_amount) order_amount30,
        sum(payment_count) payment_count30,
        sum(payment_num) payment_num30,
        sum(payment_amount) payment_amount30,
        sum(refund_count) refund_count30,
        sum(refund_num) refund_num30,
        sum(refund_amount) refund_amount30,
        sum(cart_count) cart_count30,
        sum(favor_count) favor_count30,
        sum(appraise_good_count) appraise_good_count30,
        sum(appraise_mid_count) appraise_mid_count30,
        sum(appraise_bad_count) appraise_bad_count30,
        sum(appraise_default_count) appraise_default_count30
    from dws_sku_action_daycount
    where dt >= date_add ('2020-06-14', -30)
    group by sku_id
)new
on new.sku_id = old.sku_id
left join
(select * from dwd_dim_sku_info where dt='2020-06-14') sku_info
on nvl(new.sku_id,old.sku_id)= sku_info.id;

select * from dwt_sku_topic limit 5;

drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_day_count` bigint COMMENT '当日曝光次数',
    `order_day_count` bigint COMMENT '当日下单次数',
    `order_day_amount` decimal(20,2) COMMENT '当日下单金额',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20,2) COMMENT '当日支付金额',
    `display_count` bigint COMMENT '累积曝光次数',
    `order_count` bigint COMMENT '累积下单次数',
    `order_amount` decimal(20,2) COMMENT '累积下单金额',
    `payment_count` bigint COMMENT '累积支付次数',
    `payment_amount` decimal(20,2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.activity_type,old.activity_type),
    nvl(new.start_time,old.start_time),
    nvl(new.end_time,old.end_time),
    nvl(new.create_time,old.create_time),
    nvl(new.display_count,0),
    nvl(new.order_count,0),
    nvl(new.order_amount,0.0),
    nvl(new.payment_count,0),
    nvl(new.payment_amount,0.0),
    nvl(new.display_count,0)+nvl(old.display_count,0),
    nvl(new.order_count,0)+nvl(old.order_count,0),
    nvl(new.order_amount,0.0)+nvl(old.order_amount,0.0),
    nvl(new.payment_count,0)+nvl(old.payment_count,0),
    nvl(new.payment_amount,0.0)+nvl(old.payment_amount,0.0)
from
(
    select
        *
    from dwt_activity_topic
)old
full outer join
(
    select
        *
    from dws_activity_info_daycount
    where dt='2020-06-14'
)new
on old.id=new.id;

select * from dwt_activity_topic limit 5;

drop table if exists dwt_area_topic;
create external table dwt_area_topic(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` string COMMENT '当天活跃设备数',
    `login_last_30d_count` string COMMENT '最近30天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `order_last_30d_count` bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount` decimal(16,2) COMMENT '最近30天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额',
    `payment_last_30d_count` bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16,2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_area_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_area_topic
select
    nvl(old.id,new.id),
    nvl(old.province_name,new.province_name),
    nvl(old.area_code,new.area_code),
    nvl(old.iso_code,new.iso_code),
    nvl(old.region_id,new.region_id),
    nvl(old.region_name,new.region_name),
    nvl(new.login_day_count,0),
    nvl(new.login_last_30d_count,0),
    nvl(new.order_day_count,0),
    nvl(new.order_day_amount,0.0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0.0),
    nvl(new.payment_day_count,0),
    nvl(new.payment_day_amount,0.0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0.0)
from
(
    select
        *
    from dwt_area_topic
)old
full outer join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        region_id,
        region_name,
        sum(if(dt='2020-06-14',login_count,0)) login_day_count,
        sum(if(dt='2020-06-14',order_count,0)) order_day_count,
        sum(if(dt='2020-06-14',order_amount,0.0)) order_day_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_day_count,
        sum(if(dt='2020-06-14',payment_amount,0.0)) payment_day_amount,
        sum(login_count) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_area_stats_daycount
    where dt>=date_add('2020-06-14',-30)
    group by id,province_name,area_code,iso_code,region_id,region_name
)new
on old.id=new.id;

select * from dwt_area_topic limit 5;

select * from dwt_uv_topic limit 5;
select * from dwt_user_topic limit 5;
select * from dwt_sku_topic limit 5;
select * from dwt_activity_topic limit 5;
select * from dwt_area_topic limit 5;

drop table if exists ads_uv_count;
create external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';

insert into table ads_uv_count
select
    '2020-06-14' dt,
    daycount.ct,
    wkcount.ct,
    mncount.ct,
    if(date_add(next_day('2020-06-14','MO'),-1)='2020-06-14','Y','N') ,
    if(last_day('2020-06-14')='2020-06-14','Y','N')
from
(
    select
        '2020-06-14' dt,
        count(*) ct
    from dwt_uv_topic
    where login_date_last='2020-06-14'
)daycount join
(
    select
        '2020-06-14' dt,
        count (*) ct
    from dwt_uv_topic
    where login_date_last>=date_add(next_day('2020-06-14','MO'),-7)
    and login_date_last<= date_add(next_day('2020-06-14','MO'),-1)
) wkcount on daycount.dt=wkcount.dt
join
(
    select
        '2020-06-14' dt,
        count (*) ct
    from dwt_uv_topic
    where date_format(login_date_last,'yyyy-MM')=date_format('2020-06-14','yyyy-MM')
)mncount on daycount.dt=mncount.dt;

select * from ads_uv_count;

drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

insert into table ads_new_mid_count
select
    '2020-06-14',
    count(*)
from dwt_uv_topic
where login_date_first='2020-06-14';

select * from ads_new_mid_count;

drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate
(
     `stat_date`          string comment '统计日期',
     `create_date`       string  comment '设备新增日期',
     `retention_day`     int comment '截止当前日期留存天数',
     `retention_count`    bigint comment  '留存数量',
     `new_mid_count`     bigint comment '设备新增数量',
     `retention_ratio`   decimal(16,2) comment '留存率'
)  COMMENT '留存率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

insert into table ads_user_retention_day_rate
select
    '2020-06-15',
    date_add('2020-06-15',-1),
    1,--留存天数
    sum(if(login_date_first=date_add('2020-06-15',-1) and login_date_last='2020-06-15',1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-1),1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-1) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-1),1,0))*100
from dwt_uv_topic

union all

select
    '2020-06-15',
    date_add('2020-06-15',-2),
    2,
    sum(if(login_date_first=date_add('2020-06-15',-2) and login_date_last='2020-06-15',1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-2),1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-2) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-2),1,0))*100
from dwt_uv_topic

union all

select
    '2020-06-15',
    date_add('2020-06-15',-3),
    3,
    sum(if(login_date_first=date_add('2020-06-15',-3) and login_date_last='2020-06-15',1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-3),1,0)),
    sum(if(login_date_first=date_add('2020-06-15',-3) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-3),1,0))*100
from dwt_uv_topic;

select * from ads_user_retention_day_rate;

drop table if exists ads_silent_count;
create external
    table ads_silent_count(
    `dt` string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';

insert into table ads_silent_count
select
    '2020-06-14',
    count(*)
from dwt_uv_topic
where login_date_first=login_date_last
and login_date_last<=date_add('2020-06-14',-7);

select * from ads_silent_count;

drop table if exists ads_back_count;
create external table ads_back_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

insert into table ads_back_count
select
    '2020-06-25',
    concat(date_add(next_day('2020-06-25','MO'),-7),'_', date_add(next_day('2020-06-25','MO'),-1)),
    count(*)
from
(
    select
        mid_id
    from dwt_uv_topic
    where login_date_last>=date_add(next_day('2020-06-25','MO'),-7)
    and login_date_last<= date_add(next_day('2020-06-25','MO'),-1)
    and login_date_first<date_add(next_day('2020-06-25','MO'),-7)
)current_wk
left join
(
    select
        mid_id
    from dws_uv_detail_daycount
    where dt>=date_add(next_day('2020-06-25','MO'),-7*2)
    and dt<= date_add(next_day('2020-06-25','MO'),-7-1)
    group by mid_id
)last_wk
on current_wk.mid_id=last_wk.mid_id
where last_wk.mid_id is null;

select * from ads_back_count;

drop table if exists ads_wastage_count;
create external table ads_wastage_count(
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) COMMENT '流失用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';

insert into table ads_wastage_count
select
     '2020-06-25',
     count(*)
from
(
    select
        mid_id
    from dwt_uv_topic
    where login_date_last<=date_add('2020-06-25',-7)
    group by mid_id
)t1;

select * from ads_wastage_count;

drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count(
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '活跃用户数'
) COMMENT '最近连续三周活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

insert into table ads_continuity_wk_count
select
    '2020-06-25',
    concat(date_add(next_day('2020-06-25','MO'),-7*3),'_',date_add(next_day('2020-06-25','MO'),-1)),
    count(*)
from
(
    select
        mid_id
    from
    (
        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-06-25','monday'),-7)
        and dt<=date_add(next_day('2020-06-25','monday'),-1)
        group by mid_id

        union all

        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-06-25','monday'),-7*2)
        and dt<=date_add(next_day('2020-06-25','monday'),-7-1)
        group by mid_id

        union all

        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-06-25','monday'),-7*3)
        and dt<=date_add(next_day('2020-06-25','monday'),-7*2-1)
        group by mid_id
    )t1
    group by mid_id
    having count(*)=3
)t2;

select * from ads_continuity_wk_count;

drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '最近七天内连续三天活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';

insert into table ads_continuity_uv_count
select
    '2020-06-16',
    concat(date_add('2020-06-16',-6),'_','2020-06-16'),
    count(*)
from
(
    select mid_id
    from
    (
        select mid_id
        from
        (
            select
                mid_id,
                date_sub(dt,rank) date_dif
            from
            (
                select
                    mid_id,
                    dt,
                    rank() over(partition by mid_id order by dt) rank
                from dws_uv_detail_daycount
                where dt>=date_add('2020-06-16',-6) and dt<='2020-06-16'
            )t1
        )t2
        group by mid_id,date_dif
        having count(*)>=3
    )t3
    group by mid_id
)t4;

select * from ads_continuity_uv_count;

drop table if exists ads_user_topic;
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(16,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(16,2) COMMENT '会员付费率',
    `day_new_users2users` decimal(16,2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_topic';

insert into table ads_user_topic
select
    '2020-06-14',
    sum(if(login_date_last='2020-06-14',1,0)),
    sum(if(login_date_first='2020-06-14',1,0)),
    sum(if(payment_date_first='2020-06-14',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    sum(if(login_date_last='2020-06-14',1,0))/count(*),
    sum(if(payment_count>0,1,0))/count(*),
    sum(if(login_date_first='2020-06-14',1,0))/sum(if(login_date_last='2020-06-14',1,0))
from dwt_user_topic;

select * from ads_user_topic;

drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `home_count`  bigint COMMENT '浏览首页人数',
    `good_detail_count` bigint COMMENT '浏览商品详情页人数',
    `home2good_detail_convert_ratio` decimal(16,2) COMMENT '首页到商品详情转化率',
    `cart_count` bigint COMMENT '加入购物车的人数',
    `good_detail2cart_convert_ratio` decimal(16,2) COMMENT '商品详情页到加入购物车转化率',
    `order_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(16,2) COMMENT '加入购物车到下单转化率',
    `payment_amount` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(16,2) COMMENT '下单到支付的转化率'
) COMMENT '漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';

with
tmp_uv as
(
    select
        '2020-06-14' dt,
        sum(if(array_contains(pages,'home'),1,0)) home_count,
        sum(if(array_contains(pages,'good_detail'),1,0)) good_detail_count
    from
    (
        select
            mid_id,
            collect_set(page_id) pages
        from dwd_page_log
        where dt='2020-06-14'
        and page_id in ('home','good_detail')
        group by mid_id
    )tmp
),
tmp_cop as
(
    select
        '2020-06-14' dt,
        sum(if(cart_count>0,1,0)) cart_count,
        sum(if(order_count>0,1,0)) order_count,
        sum(if(payment_count>0,1,0)) payment_count
    from dws_user_action_daycount
    where dt='2020-06-14'
)
insert into table ads_user_action_convert_day
select
    tmp_uv.dt,
    tmp_uv.home_count,
    tmp_uv.good_detail_count,
    tmp_uv.good_detail_count/tmp_uv.home_count*100,
    tmp_cop.cart_count,
    tmp_cop.cart_count/tmp_uv.good_detail_count*100,
    tmp_cop.order_count,
    tmp_cop.order_count/tmp_cop.cart_count*100,
    tmp_cop.payment_count,
    tmp_cop.payment_count/tmp_cop.order_count*100
from tmp_uv
join tmp_cop
on tmp_uv.dt=tmp_cop.dt;

select * from ads_user_action_convert_day;

drop table if exists ads_product_info;
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';

insert into table ads_product_info
select
    '2020-06-14' dt,
    sku_num,
    spu_num
from
(
    select
        '2020-06-14' dt,
        count(*) sku_num
    from
        dwt_sku_topic
) tmp_sku_num
join
(
    select
        '2020-06-14' dt,
        count(*) spu_num
    from
    (
        select
            spu_id
        from
            dwt_sku_topic
        group by
            spu_id
    ) tmp_spu_id
) tmp_spu_num
on tmp_sku_num.dt=tmp_spu_num.dt;

select * from ads_product_info;

drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品销量排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';

insert into table ads_product_sale_topN
select
    '2020-06-14' dt,
    sku_id,
    payment_amount
from
    dws_sku_action_daycount
where
    dt='2020-06-14'
order by payment_amount desc
limit 10;

select * from ads_product_sale_topN;

drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';

insert into table ads_product_favor_topN
select
    '2020-06-14' dt,
    sku_id,
    favor_count
from
    dws_sku_action_daycount
where
    dt='2020-06-14'
order by favor_count desc
limit 10;

select * from ads_product_favor_topN;

drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_count` bigint COMMENT '加入购物车次数'
) COMMENT '商品加入购物车排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';

insert into table ads_product_cart_topN
select
    '2020-06-14' dt,
    sku_id,
    cart_count
from
    dws_sku_action_daycount
where
    dt='2020-06-14'
order by cart_count desc
limit 10;

select * from ads_product_cart_topN;

drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(16,2) COMMENT '退款率'
) COMMENT '商品退款率排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';

insert into table ads_product_refund_topN
select
    '2020-06-14',
    sku_id,
    refund_last_30d_count/payment_last_30d_count*100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc
limit 10;

select * from ads_product_refund_topN;

drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16,2) COMMENT '差评率'
) COMMENT '商品差评率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';

insert into table ads_appraise_bad_topN
select
    '2020-06-14' dt,
    sku_id,
appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) appraise_bad_ratio
from
    dws_sku_action_daycount
where
    dt='2020-06-14'
order by appraise_bad_ratio desc
limit 10;

select * from ads_appraise_bad_topN;

drop table if exists ads_order_daycount;
create external table ads_order_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users bigint comment '单日下单用户数'
) comment '下单数目统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_order_daycount';

insert into table ads_order_daycount
select
    '2020-06-14',
    sum(order_count),
    sum(order_amount),
    sum(if(order_count>0,1,0))
from dws_user_action_daycount
where dt='2020-06-14';

select * from ads_order_daycount;

drop table if exists ads_payment_daycount;
create external table ads_payment_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日支付笔数',
    order_amount bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count bigint comment '单日支付商品数',
    payment_avg_time decimal(16,2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_payment_daycount';

insert into table ads_payment_daycount
select
    tmp_payment.dt,
    tmp_payment.payment_count,
    tmp_payment.payment_amount,
    tmp_payment.payment_user_count,
    tmp_skucount.payment_sku_count,
    tmp_time.payment_avg_time
from
(
    select
        '2020-06-14' dt,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(payment_count>0,1,0)) payment_user_count
    from dws_user_action_daycount
    where dt='2020-06-14'
)tmp_payment
join
(
    select
        '2020-06-14' dt,
        sum(if(payment_count>0,1,0)) payment_sku_count
    from dws_sku_action_daycount
    where dt='2020-06-14'
)tmp_skucount on tmp_payment.dt=tmp_skucount.dt
join
(
    select
        '2020-06-14' dt,
        sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time
    from dwd_fact_order_info
    where dt='2020-06-14'
    and payment_time is not null
)tmp_time on tmp_payment.dt=tmp_time.dt;

select * from ads_payment_daycount;

drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(16,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(16,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期'
) COMMENT '品牌复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

with
tmp_order as
(
    select
        user_id,
        order_stats_struct.sku_id sku_id,
        order_stats_struct.order_count order_count
    from dws_user_action_daycount lateral view explode(order_detail_stats) tmp as order_stats_struct
    where date_format(dt,'yyyy-MM')=date_format('2020-06-14','yyyy-MM')
),
tmp_sku as
(
    select
        id,
        tm_id,
        category1_id,
        category1_name
    from dwd_dim_sku_info
    where dt='2020-06-14'
)
insert into table ads_sale_tm_category1_stat_mn
select
    tm_id,
    category1_id,
    category1_name,
    sum(if(order_count>=1,1,0)) buycount,
    sum(if(order_count>=2,1,0)) buyTwiceLast,
    sum(if(order_count>=2,1,0))/sum( if(order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(order_count>=3,1,0))  buy3timeLast  ,
    sum(if(order_count>=3,1,0))/sum( if(order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2020-06-14' ,'yyyy-MM') stat_mn,
    '2020-06-14' stat_date
from
(
    select
        tmp_order.user_id,
        tmp_sku.category1_id,
        tmp_sku.category1_name,
        tmp_sku.tm_id,
        sum(order_count) order_count
    from tmp_order
    join tmp_sku
    on tmp_order.sku_id=tmp_sku.id
    group by tmp_order.user_id,tmp_sku.category1_id,tmp_sku.category1_name,tmp_sku.tm_id
)tmp
group by tm_id, category1_id, category1_name;

select * from ads_sale_tm_category1_stat_mn;

drop table if exists ads_area_topic;
create external table ads_area_topic(
    `dt` string COMMENT '统计日期',
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` bigint COMMENT '当天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_area_topic/';

insert into table ads_area_topic
select
    '2020-06-14',
    id,
    province_name,
    area_code,
    iso_code,
    region_id,
    region_name,
    login_day_count,
    order_day_count,
    order_day_amount,
    payment_day_count,
    payment_day_amount
from dwt_area_topic;

select * from ads_area_topic;

select * from ads_uv_count where dt='2020-06-15';
select * from ads_new_mid_count;
select * from ads_silent_count where dt='2020-06-15';
select * from ads_back_count where dt='2020-06-15';
select * from ads_wastage_count where dt='2020-06-15';
select * from ads_user_retention_day_rate;
select * from ads_continuity_wk_count where dt='2020-06-15';
select * from ads_continuity_uv_count where dt='2020-06-15';
select * from ads_user_topic where dt='2020-06-15';
select * from ads_user_action_convert_day where dt='2020-06-15';
select * from ads_product_info where dt='2020-06-15';
select * from ads_product_sale_topN where dt='2020-06-15';
select * from ads_product_favor_topN where dt='2020-06-15';
select * from ads_product_cart_topN where dt='2020-06-15';
select * from ads_product_refund_topN where dt='2020-06-15';
select * from ads_appraise_bad_topN where dt='2020-06-15';
select * from ads_order_daycount where dt='2020-06-15';
select * from ads_payment_daycount where dt='2020-06-15';
select * from ads_sale_tm_category1_stat_mn;
select * from ads_area_topic where dt='2020-06-15';


