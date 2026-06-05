package com.example.product_microservice.service;

import com.example.product_microservice.dto.CreateProductDto;

public interface ProductService {

    String createProduct(CreateProductDto createProductDto);
}
