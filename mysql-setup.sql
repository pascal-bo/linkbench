create database linkdb0;
use linkdb0;

CREATE TABLE `linktable` (
    `id1`        bigint(20) unsigned NOT NULL DEFAULT '0',
    `id2`        bigint(20) unsigned NOT NULL DEFAULT '0',
    `link_type`  bigint(20) unsigned NOT NULL DEFAULT '0',
    `visibility` tinyint(3) NOT NULL DEFAULT '0',
    `data`       varbinary(255) NOT NULL DEFAULT '',
    `time`       bigint(20) unsigned NOT NULL DEFAULT '0',
    `version`    int(11) unsigned NOT NULL DEFAULT '0',
    PRIMARY KEY (link_type, `id1`, `id2`),
    KEY          `id1_type` (`id1`,`link_type`,`visibility`,`time`,`id2`,`version`,`data`)
) ENGINE=InnoDB PARTITION BY key(id1) PARTITIONS 16;

CREATE TABLE `counttable` (
    `id`        bigint(20) unsigned NOT NULL DEFAULT '0',
    `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
    `count`     int(10) unsigned NOT NULL DEFAULT '0',
    `time`      bigint(20) unsigned NOT NULL DEFAULT '0',
    `version`   bigint(20) unsigned NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`, `link_type`)
) ENGINE=InnoDB;

CREATE TABLE `nodetable` (
    `id`      bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    `type`    int(10) unsigned NOT NULL,
    `version` bigint(20) unsigned NOT NULL,
    `time`    int(10) unsigned NOT NULL,
    `data`    blob NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB;