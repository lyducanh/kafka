package com.example.clickstream.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserActivity {

    private int userId;
    private String userName;
    private String zipcode;

    @Builder.Default
    private List<String> interests = new ArrayList<>();

    private String searchTerm;
    private String page;

    @Builder.Default
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant activityTime = Instant.now();

    public UserActivity(int userId, String userName, String zipcode, String[] interests, String searchTerm, String page) {
        this.userId = userId;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = (interests != null) ? new ArrayList<>(Arrays.asList(interests)) : new ArrayList<>();
        this.searchTerm = searchTerm;
        this.page = page;
        this.activityTime = Instant.now();
    }

    public UserActivity updateSearch(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }
}
