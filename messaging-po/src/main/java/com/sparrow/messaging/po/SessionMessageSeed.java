/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.sparrow.messaging.po;

import com.sparrow.protocol.POJO;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

/**
 * 每个 IM 会话一行的消息排序号校准值，映射 t_messaging_seed。
 *
 * <p>仅由系统维护，选创建/更新时间，不选人员、名称或 status 字段。
 * 所有 unsigned bigint 限定在 Java 非负 long 范围，加法及 Redis 发号溢出必须拒绝。
 * 这张表不保存咨询、收件箱或 ACK/READ 进度，不能据此恢复模型任务。
 *
 * <p>sessionId 必须由调用方赋值，不使用 GeneratedValue。现有 DDL 生成器对没有
 * GeneratedValue 的自然主键缺少空值处理，此实体的生成兼容性须在后续适配中解决，
 * 不为绕过生成器而伪造自动主键策略；其输出不得直接执行。
 */
@Data
@Entity
@Table(name = "t_messaging_seed")
public class SessionMessageSeed implements POJO {
    /**
     * 由可信 IM 会话赋值的自然主键；非自动生成、非自增。
     */
    @Id
    @Column(name = "seed_key", length = 64, nullable = false, updatable = false, columnDefinition = "varchar(64) COLLATE utf8mb4_bin COMMENT 'IM会话ID，每个会话一行'")
    private String seedKey;

    /**
     * 已提交恢复基准；初始 0，正常校准为 128 的倍数，丢键恢复原子加 256。
     */
    @Column(name = "last_synced_message_id", nullable = false, columnDefinition = "bigint UNSIGNED DEFAULT 0 COMMENT '最近完成MySQL校准的业务消息ID，128的倍数'")
    private Long lastSyncedMessageId;

    /**
     * 首次插入的实际 UTC epoch 毫秒；正常写入不得为 0。
     */
    @Column(name = "gmt_create", nullable = false, updatable = false, columnDefinition = "bigint UNSIGNED DEFAULT 0 COMMENT '创建时间'")
    private Long gmtCreate;

    @Column(name = "gmt_create_user_id", nullable = false, updatable = false, columnDefinition = "bigint UNSIGNED DEFAULT 0 COMMENT '创建用户ID'")
    private Long gmtCreateUserId;

    /**
     * 创建与每次更新时的实际 UTC epoch 毫秒；正常写入不得为 0。
     */
    @Column(name = "gmt_modified", nullable = false, columnDefinition = "bigint UNSIGNED DEFAULT 0 COMMENT '更新时间'")
    private Long gmtModified;

}
