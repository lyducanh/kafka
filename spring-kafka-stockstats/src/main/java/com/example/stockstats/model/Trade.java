package com.example.stockstats.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Trade {
    private String type;    // "ASK" or "BID"
    private String ticker;  // e.g. "MMM", "AAPL", "GOOG"
    private double price;
    private int size;
}
