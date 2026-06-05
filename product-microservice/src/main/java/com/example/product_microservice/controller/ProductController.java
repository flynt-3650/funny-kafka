package com.example.product_microservice.controller;

import com.example.product_microservice.dto.CreateProductDto;
import com.example.product_microservice.service.ProductService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/product")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public String createProduct(@RequestBody CreateProductDto createProductDto) {
        return productService.createProduct(createProductDto);
    }
}
