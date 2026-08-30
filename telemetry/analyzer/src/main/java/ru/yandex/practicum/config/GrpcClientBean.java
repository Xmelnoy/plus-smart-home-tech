package ru.yandex.practicum.config;

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Component
public class GrpcClientBean {

    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub stub;

    public GrpcClientBean(@GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub stub) {
        this.stub = stub;
    }

    public HubRouterControllerGrpc.HubRouterControllerBlockingStub getStub() {
        return stub;
    }
}