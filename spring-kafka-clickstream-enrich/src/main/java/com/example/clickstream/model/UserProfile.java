package com.example.clickstream.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserProfile {

    private int userId;
    private String userName;
    private String zipcode;

    @Builder.Default
    private List<String> interests = new ArrayList<>();

    public UserProfile(int userId, String userName, String zipcode, String[] interests) {
        this.userId = userId;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = (interests != null) ? new ArrayList<>(Arrays.asList(interests)) : new ArrayList<>();
    }

    public UserProfile update(String zipcode, List<String> interests) {
        this.zipcode = zipcode;
        this.interests = (interests != null) ? new ArrayList<>(interests) : new ArrayList<>();
        return this;
    }

    public UserProfile update(String zipcode, String[] interests) {
        this.zipcode = zipcode;
        this.interests = (interests != null) ? new ArrayList<>(Arrays.asList(interests)) : new ArrayList<>();
        return this;
    }
}
