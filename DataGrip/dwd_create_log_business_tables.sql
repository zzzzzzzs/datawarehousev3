use gmall;


DROP TABLE IF EXISTS dwd_comment_info;
CREATE EXTERNAL TABLE dwd_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品sku',
    `spu_id` STRING COMMENT '商品spu',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价(好评、中评、差评、默认评价)',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_comment_info/'
TBLPROPERTIES ("parquet.compression"="lzo");


DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail (
    `id` STRING COMMENT '订单编号',
    `order_id` STRING COMMENT '订单号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'sku商品id',
    `province_id` STRING COMMENT '省份ID',
    `activity_id` STRING COMMENT '活动ID',
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `coupon_id` STRING COMMENT '优惠券ID',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `sku_num` BIGINT COMMENT '商品数量',
    `original_amount` DECIMAL(16,2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16,2) COMMENT '活动优惠分摊',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '优惠券优惠分摊',
    `split_final_amount` DECIMAL(16,2) COMMENT '最终价格分摊'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_order_refund_info;
CREATE EXTERNAL TABLE dwd_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `province_id` STRING COMMENT '地区ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_refund_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_cart_info;
CREATE EXTERNAL TABLE dwd_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品ID',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `cart_price` DECIMAL(16,2) COMMENT '加入购物车时的价格',
    `is_ordered` STRING COMMENT '是否已下单',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `order_time` STRING COMMENT '下单时间',
    `sku_num` BIGINT COMMENT '加购数量'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_cart_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_favor_info;
CREATE EXTERNAL TABLE dwd_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING  COMMENT '用户id',
    `sku_id` STRING  COMMENT 'skuid',
    `spu_id` STRING  COMMENT 'spuid',
    `is_cancel` STRING  COMMENT '是否取消',
    `create_time` STRING  COMMENT '收藏时间',
    `cancel_time` STRING  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_favor_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_coupon_use;
CREATE EXTERNAL TABLE dwd_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'userid',
    `order_id` STRING  COMMENT '订单id',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_coupon_use/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_payment_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_refund_payment;
CREATE EXTERNAL TABLE dwd_refund_payment (
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单编号',
    `sku_id` STRING COMMENT 'SKU编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `refund_amount` DECIMAL(16,2) COMMENT '退款金额',
    `refund_status` STRING COMMENT '退款状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '回调时间'--支付接口回调时间，即支付成功时间
) COMMENT '退款事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_refund_payment/'
TBLPROPERTIES ("parquet.compression"="lzo");

DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info(
    `id` STRING COMMENT '编号',
    `order_status` STRING COMMENT '订单状态',
    `user_id` STRING COMMENT '用户ID',
    `province_id` STRING COMMENT '地区ID',
    `payment_way` STRING COMMENT '支付方式',
    `delivery_address` STRING COMMENT '邮寄地址',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `tracking_no` STRING COMMENT '物流单号',
    `create_time` STRING COMMENT '创建时间(未支付状态)',
    `payment_time` STRING COMMENT '支付时间(已支付状态)',
    `cancel_time` STRING COMMENT '取消时间(已取消状态)',
    `finish_time` STRING COMMENT '完成时间(已完成状态)',
    `refund_time` STRING COMMENT '退款时间(退款中状态)',
    `refund_finish_time` STRING COMMENT '退款完成时间(退款完成状态)',
    `expire_time` STRING COMMENT '过期时间',
    `feight_fee` DECIMAL(16,2) COMMENT '运费',
    `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免',
    `original_amount` DECIMAL(16,2) COMMENT '订单原始价格',
    `final_amount` DECIMAL(16,2) COMMENT '订单最终价格'
) COMMENT '订单事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
TBLPROPERTIES ("parquet.compression"="lzo");
