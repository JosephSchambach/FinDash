package com.findash.controller;

import com.findash.services.getAssetData;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Map;


@RestController
@RequestMapping("/api")
public class dataFetcher {
    // Simple endpoint to test the setup
    @GetMapping("/hello")
    public String hello() {
        return "Hello, world!";
    }

    @PostMapping("/data")
    public String[] data(@RequestBody Map<String, String> payload) {
        System.out.println("Received payload: " + payload);
        try {
            String symbol = payload.get("symbol");
            getAssetData assetDataService = new getAssetData();
            return assetDataService.fetchData(symbol);
            // return "Data for symbol: " + symbol + " is " + data;
        } catch (Exception e) {
            return new String[]{"Error processing request: " + e.getMessage()};
        }
    }
}