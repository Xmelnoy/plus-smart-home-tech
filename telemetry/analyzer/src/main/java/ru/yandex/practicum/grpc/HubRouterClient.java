package ru.yandex.practicum.grpc;

import com.google.protobuf.Timestamp;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.config.GrpcClientBean;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubRouterClient {

    private final GrpcClientBean grpcClientBean;

    public void sendDeviceAction(String hubId, String scenarioName, DeviceActionProto action) {
        long nowMillis = System.currentTimeMillis();

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(action)
                .setTimestamp(toTimestamp(nowMillis))
                .build();

        grpcClientBean.getStub().handleDeviceAction(request);
        log.info("Device action sent: hub={}, scenario={}, sensor={}, type={}",
                hubId, scenarioName, action.getSensorId(), action.getType());
    }

    private Timestamp toTimestamp(long millis) {
        return Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1_000_000))
                .build();
    }
}