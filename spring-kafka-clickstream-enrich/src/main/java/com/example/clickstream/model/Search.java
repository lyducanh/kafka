package com.example.clickstream.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Search {

    private int userId;
    private String searchTerms;

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp = Instant.now();

    public Search(int userId, String searchTerms) {
        this.userId = userId;
        this.searchTerms = searchTerms;
        this.timestamp = Instant.now();
    }
}
