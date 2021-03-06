/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.dashboard.repository.apollo;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.rule.FlowRuleEntity;
import com.alibaba.csp.sentinel.dashboard.rule.DynamicRuleProvider;
import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.datasource.apollo.ApolloDataSource;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.ctrip.framework.apollo.openapi.client.ApolloOpenApiClient;
import com.ctrip.framework.apollo.openapi.dto.OpenItemDTO;
import com.ctrip.framework.apollo.openapi.dto.OpenNamespaceDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author hantianwei@gmail.com
 * @since 1.5.0
 */
@Component("flowRuleApolloProvider")
public class FlowRuleApolloProvider implements DynamicRuleProvider<List<FlowRuleEntity>> {

    @Autowired
    private ApolloOpenApiClient apolloOpenApiClient;
    @Autowired
    private Converter<String, List<FlowRuleEntity>> converter;

    private String appId;

    public FlowRuleApolloProvider setAppId(String appId) {
        this.appId = appId;
        return this;
    }

    @Override
    public List<FlowRuleEntity> getRules(String appName) throws Exception {
        String flowDataId = ApolloConfigUtil.getFlowDataId(appName);
        OpenNamespaceDTO openNamespaceDTO = apolloOpenApiClient.getNamespace(appId, "DEV", "default", "application");
        String rules = openNamespaceDTO
            .getItems()
            .stream()
            .filter(p -> p.getKey().equals(flowDataId))
            .map(OpenItemDTO::getValue)
            .findFirst()
            .orElse("");

        if (StringUtil.isEmpty(rules)) {
            return new ArrayList<>();
        }
        return converter.convert(rules);
    }

    @Bean
    public ApolloDataSource apolloDataSource(ObjectMapper objectMapper) {
        // Apollo ????????????????????????????????????????????? application.yaml ??????????????????
        String appId = "SampleApp"; // Apollo ?????????????????????????????????????????? spring.application.name ????????????
        String serverAddress = "http://localhost:8070"; // Apollo Meta ???????????????
        String namespace = "application"; // Apollo ????????????
        String flowRuleKey = "SampleApp-flow-rules"; // Apollo ???????????? KEY

        // ?????? ApolloDataSource ??????
        System.setProperty("app.id", appId);
        System.setProperty("apollo.meta", serverAddress);
        ApolloDataSource<List<FlowRule>> apolloDataSource = new ApolloDataSource<>(namespace, flowRuleKey, "",
                new Converter<String, List<FlowRule>>() { // ???????????????????????? Apollo ?????????????????? FlowRule ??????
                    @Override
                    public List<FlowRule> convert(String value) {
                        try {
                            return Arrays.asList(objectMapper.readValue(value, FlowRule[].class));
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return Lists.newArrayList();
                    }
                });

        // ????????? FlowRuleManager ???
        FlowRuleManager.register2Property(apolloDataSource.getProperty());
        return apolloDataSource;
    }
}
