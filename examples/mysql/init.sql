-- 只在第一次运行容器时会执行，重启不执行

-- 设置了表的默认字符集为 utf8 并且通过 UTF-8 编码发送指令，存入数据库的数据仍然是乱码，可能是 connection 连接层上可能出了问题。
-- 各种设置无果，只能在指令前加入 【SET NAMES 'utf8'】，解决中文乱码问题，相当于如下三句指令：
-- SET character_set_client = utf8;
-- SET character_set_results = utf8;
-- SET character_set_connection = utf8;
SET NAMES 'utf8';

create database `flink` character set utf8 collate utf8_general_ci;

use `flink`;

create table `case3` (
  `id` int not null AUTO_INCREMENT,
  `name` varchar(64) COLLATE utf8mb4_unicode_ci not null default '' comment '姓名',
  primary key (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

insert into `case3` (`name`) values ('李雷'), ('韩梅梅');