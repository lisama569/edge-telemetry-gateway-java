package com.github.lisama569.edgetelemetrygatewayjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 启用网关配置项。
 */
@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {
}
