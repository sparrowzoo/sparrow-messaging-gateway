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

package com.sparrow.messaging.infrastructure.service.netty;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WebSocketIndexPageHandlerTest {
    @Test
    void optionalIndexPageAlwaysProducesAnHttpResponse() {
        var channel = new EmbeddedChannel(new WebSocketIndexPageHandler("/websocket"));
        try {
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            request.headers().set(HttpHeaderNames.HOST, "localhost");
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response, "Missing optional resource must produce HTTP 404 rather than a silent disconnect");
            try {
                assertEquals(WebSocketServerIndexPage.class.getResource("/index.html") == null
                        ? HttpResponseStatus.NOT_FOUND : HttpResponseStatus.OK, response.status());
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
