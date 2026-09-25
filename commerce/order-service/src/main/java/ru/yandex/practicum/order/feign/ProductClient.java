package ru.yandex.practicum.order.feign;

import inventory.ReserveRequest;
import inventory.ReserveResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import product.ProductDto;


@FeignClient(name = "product-service")
public interface ProductClient {

    @GetMapping("/api/products/{id}")
    ProductDto getProductById(@PathVariable("id") Long id);

    @PostMapping("/api/inventory/release")
    ReserveResponse releaseStock(@RequestBody ReserveRequest request);
}