package com.example.wordcount.model;

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
public class WordCountResult {

    private String word;
    private Long count;

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant updatedAt = Instant.now();
}
