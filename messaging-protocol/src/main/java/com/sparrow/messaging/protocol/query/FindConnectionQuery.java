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

package com.sparrow.messaging.protocol.query;

import com.sparrow.protocol.Query;
import com.sparrow.protocol.enums.DeviceType;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.Data;

/**
 * 服务端连接查询条件，不是客户端自报身份的 HTTP 入参。
 */
@Data
public class FindConnectionQuery implements Query {
    @NotNull
    @PositiveOrZero
    private Long userId;

    private Integer category;

    @NotNull
    @PositiveOrZero
    private DeviceType deviceType;
}
