/*
    TODO 第一章：环境准备
    1.1 用户信息表
    1.2 商品信息表
    1.3 商品分类信息表
    1.4 订单信息表
    1.5 订单明细表
    1.6 登录明细表
    1.7 商品价格变更明细表
    1.8 配送信息表
    1.9 好友关系表
    1.10 收藏信息表
*/
/*TODO 建表&插入数据：1.1 用户信息表*/
--建表
DROP TABLE IF EXISTS user_info;
create table user_info(
    `user_id`  string COMMENT '用户id',
    `gender`   string COMMENT '性别',
    `birthday` string COMMENT '生日'
) COMMENT '用户信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table user_info
values ('101', '男', '1990-01-01'),
       ('102', '女', '1991-02-01'),
       ('103', '女', '1992-03-01'),
       ('104', '男', '1993-04-01'),
       ('105', '女', '1994-05-01'),
       ('106', '男', '1995-06-01'),
       ('107', '女', '1996-07-01'),
       ('108', '男', '1997-08-01'),
       ('109', '女', '1998-09-01'),
       ('1010', '男', '1999-10-01');
/*TODO 建表&插入数据：1.2 商品信息表*/
--创建表
DROP TABLE IF EXISTS sku_info;
CREATE TABLE sku_info(
    `sku_id`      string COMMENT '商品id',
    `name`        string COMMENT '商品名称',
    `category_id` string COMMENT '所属分类id',
    `from_date`   string COMMENT '上架日期',
    `price`       double COMMENT '商品单价'
) COMMENT '商品属性表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table sku_info
values ('1', 'xiaomi 10', '1', '2020-01-01', 2000),
       ('2', '手机壳', '1', '2020-02-01', 10),
       ('3', 'apple 12', '1', '2020-03-01', 5000),
       ('4', 'xiaomi 13', '1', '2020-04-01', 6000),
       ('5', '破壁机', '2', '2020-01-01', 500),
       ('6', '洗碗机', '2', '2020-02-01', 2000),
       ('7', '热水壶', '2', '2020-03-01', 100),
       ('8', '微波炉', '2', '2020-04-01', 600),
       ('9', '自行车', '3', '2020-01-01', 1000),
       ('10', '帐篷', '3', '2020-02-01', 100),
       ('11', '烧烤架', '3', '2020-02-01', 50),
       ('12', '遮阳伞', '3', '2020-03-01', 20);
/*TODO 建表&插入数据：1.3 商品分类信息表*/
--创建表
DROP TABLE IF EXISTS category_info;
create table category_info(
    `category_id`   string,
    `category_name` string
) COMMENT '品类表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table category_info
values ('1','数码'),
       ('2','厨卫'),
       ('3','户外');


/*TODO 建表&插入数据：1.4 订单信息表*/
--创建表
DROP TABLE IF EXISTS order_info;
create table order_info(
    `order_id`     string COMMENT '订单id',
    `user_id`      string COMMENT '用户id',
    `create_date`  string COMMENT '下单日期',
    `total_amount` decimal(16, 2) COMMENT '订单总金额'
) COMMENT '订单表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table order_info
values ('1', '101', '2021-09-27', 29000.00),
       ('2', '101', '2021-09-28', 70500.00),
       ('3', '101', '2021-09-29', 43300.00),
       ('4', '101', '2021-09-30', 860.00),
       ('5', '102', '2021-10-01', 46180.00),
       ('6', '102', '2021-10-01', 50000.00),
       ('7', '102', '2021-10-01', 75500.00),
       ('8', '102', '2021-10-02', 6170.00),
       ('9', '103', '2021-10-02', 18580.00),
       ('10', '103', '2021-10-02', 28000.00),
       ('11', '103', '2021-10-02', 23400.00),
       ('12', '103', '2021-10-03', 5910.00),
       ('13', '104', '2021-10-03', 13000.00),
       ('14', '104', '2021-10-03', 69500.00),
       ('15', '104', '2021-10-03', 2000.00),
       ('16', '104', '2021-10-03', 5380.00),
       ('17', '105', '2021-10-04', 6210.00),
       ('18', '105', '2021-10-04', 68000.00),
       ('19', '105', '2021-10-04', 43100.00),
       ('20', '105', '2021-10-04', 2790.00),
       ('21', '106', '2021-10-04', 9390.00),
       ('22', '106', '2021-10-05', 58000.00),
       ('23', '106', '2021-10-05', 46600.00),
       ('24', '106', '2021-10-05', 5160.00),
       ('25', '107', '2021-10-05', 55350.00),
       ('26', '107', '2021-10-05', 14500.00),
       ('27', '107', '2021-10-06', 47400.00),
       ('28', '107', '2021-10-06', 6900.00),
       ('29', '108', '2021-10-06', 56570.00),
       ('30', '108', '2021-10-06', 44500.00),
       ('31', '108', '2021-10-07', 50800.00),
       ('32', '108', '2021-10-07', 3900.00),
       ('33', '109', '2021-10-07', 41480.00),
       ('34', '109', '2021-10-07', 88000.00),
       ('35', '109', '2020-10-08', 15000.00),
       ('36', '109', '2020-10-08', 9020.00),
       ('37', '1010', '2020-10-08', 9260.00),
       ('38', '1010', '2020-10-08', 12000.00),
       ('39', '1010', '2020-10-08', 23900.00),
       ('40', '1010', '2020-10-08', 6790.00);


/*TODO 建表&插入数据：1.5 订单明细表*/
--创建表
DROP TABLE IF EXISTS order_detail;
CREATE TABLE order_detail
(
    `order_detail_id` string COMMENT '订单明细id',
    `order_id`        string COMMENT '订单id',
    `sku_id`          string COMMENT '商品id',
    `create_date`     string COMMENT '下单日期',
    `price`           decimal(16, 2) COMMENT '下单时的商品单价',
    `sku_num`         int COMMENT '下单商品件数'
) COMMENT '订单明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
INSERT overwrite table order_detail
values ('1', '1', '1', '2021-09-27', 2000.00, 2),
       ('2', '1', '3', '2021-09-27', 5000.00, 5),
       ('3', '2', '4', '2021-09-28', 6000.00, 9),
       ('4', '2', '5', '2021-09-28', 500.00, 33),
       ('5', '3', '7', '2021-09-29', 100.00, 37),
       ('6', '3', '8', '2021-09-29', 600.00, 46),
       ('7', '3', '9', '2021-09-29', 1000.00, 12),
       ('8', '4', '12', '2021-09-30', 20.00, 43),
       ('9', '5', '1', '2021-10-01', 2000.00, 8),
       ('10', '5', '2', '2021-10-01', 10.00, 18),
       ('11', '5', '3', '2021-10-01', 5000.00, 6),
       ('12', '6', '4', '2021-10-01', 6000.00, 8),
       ('13', '6', '6', '2021-10-01', 2000.00, 1),
       ('14', '7', '7', '2021-10-01', 100.00, 17),
       ('15', '7', '8', '2021-10-01', 600.00, 48),
       ('16', '7', '9', '2021-10-01', 1000.00, 45),
       ('17', '8', '10', '2021-10-02', 100.00, 48),
       ('18', '8', '11', '2021-10-02', 50.00, 15),
       ('19', '8', '12', '2021-10-02', 20.00, 31),
       ('20', '9', '1', '2021-09-30', 2000.00, 9),
       ('21', '9', '2', '2021-10-02', 10.00, 5800),
       ('22', '10', '4', '2021-10-02', 6000.00, 1),
       ('23', '10', '5', '2021-10-02', 500.00, 24),
       ('24', '10', '6', '2021-10-02', 2000.00, 5),
       ('25', '11', '8', '2021-10-02', 600.00, 39),
       ('26', '12', '10', '2021-10-03', 100.00, 47),
       ('27', '12', '11', '2021-10-03', 50.00, 19),
       ('28', '12', '12', '2021-10-03', 20.00, 13000),
       ('29', '13', '1', '2021-10-03', 2000.00, 4),
       ('30', '13', '3', '2021-10-03', 5000.00, 1),
       ('31', '14', '4', '2021-10-03', 6000.00, 5),
       ('32', '14', '5', '2021-10-03', 500.00, 47),
       ('33', '14', '6', '2021-10-03', 2000.00, 8),
       ('34', '15', '7', '2021-10-03', 100.00, 20),
       ('35', '16', '10', '2021-10-03', 100.00, 22),
       ('36', '16', '11', '2021-10-03', 50.00, 42),
       ('37', '16', '12', '2021-10-03', 20.00, 7400),
       ('38', '17', '1', '2021-10-04', 2000.00, 3),
       ('39', '17', '2', '2021-10-04', 10.00, 21),
       ('40', '18', '4', '2021-10-04', 6000.00, 8),
       ('41', '18', '5', '2021-10-04', 500.00, 28),
       ('42', '18', '6', '2021-10-04', 2000.00, 3),
       ('43', '19', '7', '2021-10-04', 100.00, 55),
       ('44', '19', '8', '2021-10-04', 600.00, 11),
       ('45', '19', '9', '2021-10-04', 1000.00, 31),
       ('46', '20', '11', '2021-10-04', 50.00, 45),
       ('47', '20', '12', '2021-10-04', 20.00, 27),
       ('48', '21', '1', '2021-10-04', 2000.00, 2),
       ('49', '21', '2', '2021-10-04', 10.00, 39),
       ('50', '21', '3', '2021-10-04', 5000.00, 1),
       ('51', '22', '4', '2021-10-05', 6000.00, 8),
       ('52', '22', '5', '2021-10-05', 500.00, 20),
       ('53', '23', '7', '2021-10-05', 100.00, 58),
       ('54', '23', '8', '2021-10-05', 600.00, 18),
       ('55', '23', '9', '2021-10-05', 1000.00, 30),
       ('56', '24', '10', '2021-10-05', 100.00, 27),
       ('57', '24', '11', '2021-10-05', 50.00, 28),
       ('58', '24', '12', '2021-10-05', 20.00, 53),
       ('59', '25', '1', '2021-10-05', 2000.00, 5),
       ('60', '25', '2', '2021-10-05', 10.00, 35),
       ('61', '25', '3', '2021-10-05', 5000.00, 9),
       ('62', '26', '4', '2021-10-05', 6000.00, 1),
       ('63', '26', '5', '2021-10-05', 500.00, 13),
       ('64', '26', '6', '2021-10-05', 2000.00, 1),
       ('65', '27', '7', '2021-10-06', 100.00, 30),
       ('66', '27', '8', '2021-10-06', 600.00, 19),
       ('67', '27', '9', '2021-10-06', 1000.00, 33),
       ('68', '28', '10', '2021-10-06', 100.00, 37),
       ('69', '28', '11', '2021-10-06', 50.00, 46),
       ('70', '28', '12', '2021-10-06', 20.00, 45),
       ('71', '29', '1', '2021-10-06', 2000.00, 8),
       ('72', '29', '2', '2021-10-06', 10.00, 57),
       ('73', '29', '3', '2021-10-06', 5000.00, 8),
       ('74', '30', '4', '2021-10-06', 6000.00, 3),
       ('75', '30', '5', '2021-10-06', 500.00, 33),
       ('76', '30', '6', '2021-10-06', 2000.00, 5),
       ('77', '31', '8', '2021-10-07', 600.00, 13),
       ('78', '31', '9', '2021-10-07', 1000.00, 43),
       ('79', '32', '10', '2021-10-07', 100.00, 24),
       ('80', '32', '11', '2021-10-07', 50.00, 30),
       ('81', '33', '1', '2021-10-07', 2000.00, 8),
       ('82', '33', '2', '2021-10-07', 10.00, 48),
       ('83', '33', '3', '2021-10-07', 5000.00, 5),
       ('84', '34', '4', '2021-10-07', 6000.00, 10),
       ('85', '34', '5', '2021-10-07', 500.00, 44),
       ('86', '34', '6', '2021-10-07', 2000.00, 3),
       ('87', '35', '8', '2020-10-08', 600.00, 25),
       ('88', '36', '10', '2020-10-08', 100.00, 57),
       ('89', '36', '11', '2020-10-08', 50.00, 44),
       ('90', '36', '12', '2020-10-08', 20.00, 56),
       ('91', '37', '1', '2020-10-08', 2000.00, 2),
       ('92', '37', '2', '2020-10-08', 10.00, 26),
       ('93', '37', '3', '2020-10-08', 5000.00, 1),
       ('94', '38', '6', '2020-10-08', 2000.00, 6),
       ('95', '39', '7', '2020-10-08', 100.00, 35),
       ('96', '39', '8', '2020-10-08', 600.00, 34),
       ('97', '40', '10', '2020-10-08', 100.00, 37),
       ('98', '40', '11', '2020-10-08', 50.00, 51),
       ('99', '40', '12', '2020-10-08', 20.00, 27);

/*TODO 建表&插入数据：1.6 登录明细表*/
--创建表
DROP TABLE IF EXISTS user_login_detail;
CREATE TABLE user_login_detail
(
    `user_id`    string comment '用户id',
    `ip_address` string comment 'ip地址',
    `login_ts`   string comment '登录时间',
    `logout_ts`  string comment '登出时间'
) COMMENT '用户登录明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
INSERT overwrite table user_login_detail
VALUES ('101', '180.149.130.161', '2021-09-21 08:00:00', '2021-09-27 08:30:00'),
       ('101', '180.149.130.161', '2021-09-27 08:00:00', '2021-09-27 08:30:00'),
       ('101', '180.149.130.161', '2021-09-28 09:00:00', '2021-09-28 09:10:00'),
       ('101', '180.149.130.161', '2021-09-29 13:30:00', '2021-09-29 13:50:00'),
       ('101', '180.149.130.161', '2021-09-30 20:00:00', '2021-09-30 20:10:00'),
       ('102', '120.245.11.2', '2021-09-22 09:00:00', '2021-09-27 09:30:00'),
       ('102', '120.245.11.2', '2021-10-01 08:00:00', '2021-10-01 08:30:00'),
       ('102', '180.149.130.174', '2021-10-01 07:50:00', '2021-10-01 08:20:00'),
       ('102', '120.245.11.2', '2021-10-02 08:00:00', '2021-10-02 08:30:00'),
       ('103', '27.184.97.3', '2021-09-23 10:00:00', '2021-09-27 10:30:00'),
       ('103', '27.184.97.3', '2021-10-03 07:50:00', '2021-10-03 09:20:00'),
       ('104', '27.184.97.34', '2021-09-24 11:00:00', '2021-09-27 11:30:00'),
       ('104', '27.184.97.34', '2021-10-03 07:50:00', '2021-10-03 08:20:00'),
       ('104', '27.184.97.34', '2021-10-03 08:50:00', '2021-10-03 10:20:00'),
       ('104', '120.245.11.89', '2021-10-03 08:40:00', '2021-10-03 10:30:00'),
       ('105', '119.180.192.212', '2021-10-04 09:10:00', '2021-10-04 09:30:00'),
       ('106', '119.180.192.66', '2021-10-04 08:40:00', '2021-10-04 10:30:00'),
       ('106', '119.180.192.66', '2021-10-05 21:50:00', '2021-10-05 22:40:00'),
       ('107', '219.134.104.7', '2021-09-25 12:00:00', '2021-09-27 12:30:00'),
       ('107', '219.134.104.7', '2021-10-05 22:00:00', '2021-10-05 23:00:00'),
       ('107', '219.134.104.7', '2021-10-06 09:10:00', '2021-10-06 10:20:00'),
       ('107', '27.184.97.46', '2021-10-06 09:00:00', '2021-10-06 10:00:00'),
       ('108', '101.227.131.22', '2021-10-06 09:00:00', '2021-10-06 10:00:00'),
       ('108', '101.227.131.22', '2021-10-06 22:00:00', '2021-10-06 23:00:00'),
       ('109', '101.227.131.29', '2021-09-26 13:00:00', '2021-09-27 13:30:00'),
       ('109', '101.227.131.29', '2021-10-06 08:50:00', '2021-10-06 10:20:00'),
       ('109', '101.227.131.29', '2021-10-08 09:00:00', '2021-10-08 09:10:00'),
       ('1010', '119.180.192.10', '2021-09-27 14:00:00', '2021-09-27 14:30:00'),
       ('1010', '119.180.192.10', '2021-10-09 08:50:00', '2021-10-09 10:20:00');

/*TODO 建表&插入数据：1.7 商品价格变更明细表*/
--创建表
DROP TABLE IF EXISTS sku_price_modify_detail;
CREATE TABLE sku_price_modify_detail
(
    `sku_id`      string comment '商品id',
    `new_price`   decimal(16, 2) comment '更改后的价格',
    `change_date` string comment '变动日期'
) COMMENT '商品价格变更明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table sku_price_modify_detail
values ('1', 1900, '2021-09-25'),
       ('1', 2000, '2021-09-26'),
       ('2', 80, '2021-09-29'),
       ('2', 10, '2021-09-30'),
       ('3', 4999, '2021-09-25'),
       ('3', 5000, '2021-09-26'),
       ('4', 5600, '2021-09-26'),
       ('4', 6000, '2021-09-27'),
       ('5', 490, '2021-09-27'),
       ('5', 500, '2021-09-28'),
       ('6', 1988, '2021-09-30'),
       ('6', 2000, '2021-10-01'),
       ('7', 88, '2021-09-28'),
       ('7', 100, '2021-09-29'),
       ('8', 800, '2021-09-28'),
       ('8', 600, '2021-09-29'),
       ('9', 1100, '2021-09-27'),
       ('9', 1000, '2021-09-28'),
       ('10', 90, '2021-10-01'),
       ('10', 100, '2021-10-02'),
       ('11', 66, '2021-10-01'),
       ('11', 50, '2021-10-02'),
       ('12', 35, '2021-09-28'),
       ('12', 20, '2021-09-29');


/*TODO 建表&插入数据：1.8 配送信息表*/
--创建表
DROP TABLE IF EXISTS delivery_info;
CREATE TABLE delivery_info
(
    `delivery_id` string comment '配送单id',
    `order_id`    string comment '订单id',
    `user_id`     string comment '用户id',
    `order_date`  string comment '下单日期',
    `custom_date` string comment '期望配送日期'
) COMMENT '邮寄信息表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table delivery_info
values ('1', '1', '101', '2021-09-27', '2021-09-29'),
       ('2', '2', '101', '2021-09-28', '2021-09-28'),
       ('3', '3', '101', '2021-09-29', '2021-09-30'),
       ('4', '4', '101', '2021-09-30', '2021-10-01'),
       ('5', '5', '102', '2021-10-01', '2021-10-01'),
       ('6', '6', '102', '2021-10-01', '2021-10-01'),
       ('7', '7', '102', '2021-10-01', '2021-10-03'),
       ('8', '8', '102', '2021-10-02', '2021-10-02'),
       ('9', '9', '103', '2021-10-02', '2021-10-03'),
       ('10', '10', '103', '2021-10-02', '2021-10-04'),
       ('11', '11', '103', '2021-10-02', '2021-10-02'),
       ('12', '12', '103', '2021-10-03', '2021-10-03'),
       ('13', '13', '104', '2021-10-03', '2021-10-04'),
       ('14', '14', '104', '2021-10-03', '2021-10-04'),
       ('15', '15', '104', '2021-10-03', '2021-10-03'),
       ('16', '16', '104', '2021-10-03', '2021-10-03'),
       ('17', '17', '105', '2021-10-04', '2021-10-04'),
       ('18', '18', '105', '2021-10-04', '2021-10-06'),
       ('19', '19', '105', '2021-10-04', '2021-10-06'),
       ('20', '20', '105', '2021-10-04', '2021-10-04'),
       ('21', '21', '106', '2021-10-04', '2021-10-04'),
       ('22', '22', '106', '2021-10-05', '2021-10-05'),
       ('23', '23', '106', '2021-10-05', '2021-10-05'),
       ('24', '24', '106', '2021-10-05', '2021-10-07'),
       ('25', '25', '107', '2021-10-05', '2021-10-05'),
       ('26', '26', '107', '2021-10-05', '2021-10-06'),
       ('27', '27', '107', '2021-10-06', '2021-10-06'),
       ('28', '28', '107', '2021-10-06', '2021-10-07'),
       ('29', '29', '108', '2021-10-06', '2021-10-06'),
       ('30', '30', '108', '2021-10-06', '2021-10-06'),
       ('31', '31', '108', '2021-10-07', '2021-10-09'),
       ('32', '32', '108', '2021-10-07', '2021-10-09'),
       ('33', '33', '109', '2021-10-07', '2021-10-08'),
       ('34', '34', '109', '2021-10-07', '2021-10-08'),
       ('35', '35', '109', '2021-10-08', '2021-10-10'),
       ('36', '36', '109', '2021-10-08', '2021-10-09'),
       ('37', '37', '1010', '2021-10-08', '2021-10-10'),
       ('38', '38', '1010', '2021-10-08', '2021-10-10'),
       ('39', '39', '1010', '2021-10-08', '2021-10-09'),
       ('40', '40', '1010', '2021-10-08', '2021-10-09');

/*TODO 建表&插入数据：1.9 好友关系表*/
--创建表
DROP TABLE IF EXISTS friendship_info;
CREATE TABLE friendship_info(
    `user1_id` string comment '用户1id',
    `user2_id` string comment '用户2id'
) COMMENT '用户关系表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table friendship_info
values ('101', '1010'),
       ('101', '108'),
       ('101', '106'),
       ('101', '104'),
       ('101', '102'),
       ('102', '1010'),
       ('102', '108'),
       ('102', '106'),
       ('102', '104'),
       ('102', '102'),
       ('103', '1010'),
       ('103', '108'),
       ('103', '106'),
       ('103', '104'),
       ('103', '102'),
       ('104', '1010'),
       ('104', '108'),
       ('104', '106'),
       ('104', '104'),
       ('104', '102'),
       ('105', '1010'),
       ('105', '108'),
       ('105', '106'),
       ('105', '104'),
       ('105', '102'),
       ('106', '1010'),
       ('106', '108'),
       ('106', '106'),
       ('106', '104'),
       ('106', '102'),
       ('107', '1010'),
       ('107', '108'),
       ('107', '106'),
       ('107', '104'),
       ('107', '102'),
       ('108', '1010'),
       ('108', '108'),
       ('108', '106'),
       ('108', '104'),
       ('108', '102'),
       ('109', '1010'),
       ('109', '108'),
       ('109', '106'),
       ('109', '104'),
       ('109', '102'),
       ('1010', '1010'),
       ('1010', '108'),
       ('1010', '106'),
       ('1010', '104'),
       ('1010', '102');


/*TODO 建表&插入数据：1.10 收藏信息表*/
--创建表
DROP TABLE IF EXISTS favor_info;
CREATE TABLE favor_info
(
    `user_id`     string comment '用户id',
    `sku_id`      string comment '商品id',
    `create_date` string comment '收藏日期'
) COMMENT '用户收藏表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- 插入数据
insert overwrite table favor_info
values ('101', '3', '2021-09-23'),
       ('101', '12', '2021-09-23'),
       ('101', '6', '2021-09-25'),
       ('101', '10', '2021-09-21'),
       ('101', '5', '2021-09-25'),
       ('102', '1', '2021-09-24'),
       ('102', '2', '2021-09-24'),
       ('102', '8', '2021-09-23'),
       ('102', '12', '2021-09-22'),
       ('102', '11', '2021-09-23'),
       ('102', '9', '2021-09-25'),
       ('102', '4', '2021-09-25'),
       ('102', '6', '2021-09-23'),
       ('102', '7', '2021-09-26'),
       ('103', '8', '2021-09-24'),
       ('103', '5', '2021-09-25'),
       ('103', '6', '2021-09-26'),
       ('103', '12', '2021-09-27'),
       ('103', '7', '2021-09-25'),
       ('103', '10', '2021-09-25'),
       ('103', '4', '2021-09-24'),
       ('103', '11', '2021-09-25'),
       ('103', '3', '2021-09-27'),
       ('104', '9', '2021-09-28'),
       ('104', '7', '2021-09-28'),
       ('104', '8', '2021-09-25'),
       ('104', '3', '2021-09-28'),
       ('104', '11', '2021-09-25'),
       ('104', '6', '2021-09-25'),
       ('104', '12', '2021-09-28'),
       ('105', '8', '2021-10-08'),
       ('105', '9', '2021-10-07'),
       ('105', '7', '2021-10-07'),
       ('105', '11', '2021-10-06'),
       ('105', '5', '2021-10-07'),
       ('105', '4', '2021-10-05'),
       ('105', '10', '2021-10-07'),
       ('106', '12', '2021-10-08'),
       ('106', '1', '2021-10-08'),
       ('106', '4', '2021-10-04'),
       ('106', '5', '2021-10-08'),
       ('106', '2', '2021-10-04'),
       ('106', '6', '2021-10-04'),
       ('106', '7', '2021-10-08'),
       ('107', '5', '2021-09-29'),
       ('107', '3', '2021-09-28'),
       ('107', '10', '2021-09-27'),
       ('108', '9', '2021-10-08'),
       ('108', '3', '2021-10-10'),
       ('108', '8', '2021-10-10'),
       ('108', '10', '2021-10-07'),
       ('108', '11', '2021-10-07'),
       ('109', '2', '2021-09-27'),
       ('109', '4', '2021-09-29'),
       ('109', '5', '2021-09-29'),
       ('109', '9', '2021-09-30'),
       ('109', '8', '2021-09-26'),
       ('1010', '2', '2021-09-29'),
       ('1010', '9', '2021-09-29'),
       ('1010', '1', '2021-10-01');
