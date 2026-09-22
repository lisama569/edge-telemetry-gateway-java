package com.github.lisama569.edgetelemetrygatewayjava.normalizer;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * 按协议名查找归一化器。
 */
@Component
public class NormalizerRegistry {

    private final Map<String, ProtocolNormalizer> byProtocol;

    public NormalizerRegistry(java.util.List<ProtocolNormalizer> normalizers) {
        this.byProtocol = normalizers.stream()
                .collect(Collectors.toMap(ProtocolNormalizer::protocol, n -> n));
    }

    public Optional<ProtocolNormalizer> find(String protocol) {
        return Optional.ofNullable(byProtocol.get(protocol));
    }
}
