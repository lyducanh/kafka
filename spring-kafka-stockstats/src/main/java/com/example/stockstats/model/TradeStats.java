package com.example.stockstats.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeStats {

    private String type;
    private String ticker;
    private int countTrades;
    private double sumPrice;
    private double minPrice;
    private double maxPrice;
    private double avgPrice;
    private String windowStart;
    private String windowEnd;

    public TradeStats add(Trade trade) {
        if (trade == null || trade.getTicker() == null) {
            return this;
        }

        if (this.type == null) {
            this.type = trade.getType();
        }
        if (this.ticker == null) {
            this.ticker = trade.getTicker();
        }

        if (countTrades == 0) {
            this.minPrice = trade.getPrice();
            this.maxPrice = trade.getPrice();
        } else {
            this.minPrice = Math.min(this.minPrice, trade.getPrice());
            this.maxPrice = Math.max(this.maxPrice, trade.getPrice());
        }

        this.countTrades++;
        this.sumPrice += trade.getPrice();
        computeAvgPrice();
        return this;
    }

    public TradeStats computeAvgPrice() {
        if (this.countTrades > 0) {
            this.avgPrice = Math.round((this.sumPrice / this.countTrades) * 100.0) / 100.0;
        }
        return this;
    }
}
