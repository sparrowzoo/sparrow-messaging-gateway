/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.messaging.infrastructure.service.spring;

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.AbstractSubHandler;
import com.sparrow.messaging.protocol.api.Subscriber;
import jakarta.inject.Inject;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;

/** 单例初始化完成后自行登记；从 Spring 取最终 Bean，保留鉴权和事务拦截。 */
public abstract class AbstractSpringSubscriber<T extends Event> extends AbstractSubHandler<T>
        implements BeanNameAware, SmartInitializingSingleton {
    @Inject
    private ApplicationContext applicationContext;
    private String beanName;

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @Override
    public void afterSingletonsInstantiated() {
        // 从 Subscriber<GreetingMessage> 这样的声明中取得消息类型。
        ResolvableType subscriberType = ResolvableType.forClass(getClass()).as(Subscriber.class);
        Class<?> messageClass = subscriberType.getGeneric(0).resolve();
        if (messageClass == null || subscriberType.hasUnresolvableGenerics()) {
            throw new IllegalArgumentException("Cannot resolve Subscriber message type for " + beanName);
        }
        // 登记容器最终提供的 Bean，不能登记尚未包装的 this。
        Subscriber subscriber = applicationContext.getBean(beanName, Subscriber.class);
        queueHandlerMappingContainer.put(messageClass.asSubclass(Event.class), subscriber);
    }
}
