package com.example.clickstream.topology;

import com.example.clickstream.model.PageView;
import com.example.clickstream.model.Search;
import com.example.clickstream.model.UserActivity;
import com.example.clickstream.model.UserProfile;
import com.example.clickstream.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

@Slf4j
@Component
public class ClickstreamEnrichmentTopology {

    public static final String USER_PROFILE_STORE = "user-profile-store";

    @Value("${app.kafka.topics.page-views:clicks.pages.views}")
    private String pageViewsTopic;

    @Value("${app.kafka.topics.user-profile:clicks.user.profile}")
    private String userProfileTopic;

    @Value("${app.kafka.topics.search:clicks.search}")
    private String searchTopic;

    @Value("${app.kafka.topics.user-activity:clicks.user.activity}")
    private String userActivityTopic;

    @Value("${app.streams.join-window-seconds:10}")
    private int joinWindowSeconds;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        JsonSerde<PageView> pageViewSerde = new JsonSerde<>(PageView.class);
        JsonSerde<UserProfile> userProfileSerde = new JsonSerde<>(UserProfile.class);
        JsonSerde<Search> searchSerde = new JsonSerde<>(Search.class);
        JsonSerde<UserActivity> userActivitySerde = new JsonSerde<>(UserActivity.class);

        // 1. Ingest PageViews stream
        KStream<Integer, PageView> views = builder.stream(
                pageViewsTopic,
                Consumed.with(Serdes.Integer(), pageViewSerde)
        );

        views.peek((userId, view) ->
                log.info("[STREAM-PAGEVIEW-IN] UserID={} Page='{}'",
                        userId, view != null ? view.getPage() : "null")
        );

        // 2. Ingest UserProfiles as KTable materialized in RocksDB
        KTable<Integer, UserProfile> profiles = builder.table(
                userProfileTopic,
                Consumed.with(Serdes.Integer(), userProfileSerde),
                Materialized.<Integer, UserProfile, KeyValueStore<Bytes, byte[]>>as(USER_PROFILE_STORE)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(userProfileSerde)
        );

        // 3. Ingest Searches stream
        KStream<Integer, Search> searches = builder.stream(
                searchTopic,
                Consumed.with(Serdes.Integer(), searchSerde)
        );

        searches.peek((userId, search) ->
                log.info("[STREAM-SEARCH-IN] UserID={} Terms='{}'",
                        userId, search != null ? search.getSearchTerms() : "null")
        );

        // 4. Stream-Table Join: PageView (KStream) leftJoin UserProfile (KTable)
        KStream<Integer, UserActivity> viewsWithProfile = views.leftJoin(
                profiles,
                (pageView, profile) -> {
                    Instant eventTime = (pageView != null && pageView.getTimestamp() != null)
                            ? pageView.getTimestamp()
                            : Instant.now();
                    String page = (pageView != null) ? pageView.getPage() : "";

                    if (profile != null) {
                        return UserActivity.builder()
                                .userId(profile.getUserId())
                                .userName(profile.getUserName())
                                .zipcode(profile.getZipcode())
                                .interests(profile.getInterests() != null ? profile.getInterests() : Collections.emptyList())
                                .searchTerm("")
                                .page(page)
                                .activityTime(eventTime)
                                .build();
                    } else {
                        int fallbackId = (pageView != null) ? pageView.getUserId() : -1;
                        return UserActivity.builder()
                                .userId(fallbackId)
                                .userName("Anonymous")
                                .zipcode("N/A")
                                .interests(Collections.emptyList())
                                .searchTerm("")
                                .page(page)
                                .activityTime(eventTime)
                                .build();
                    }
                },
                Joined.with(Serdes.Integer(), pageViewSerde, userProfileSerde)
        );

        // 5. Stream-Stream Join: viewsWithProfile leftJoin searches with JoinWindows
        JoinWindows joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(joinWindowSeconds));

        KStream<Integer, UserActivity> userActivityStream = viewsWithProfile.leftJoin(
                searches,
                (activity, search) -> {
                    String searchTerm = (search != null && search.getSearchTerms() != null)
                            ? search.getSearchTerms()
                            : "";
                    return UserActivity.builder()
                            .userId(activity.getUserId())
                            .userName(activity.getUserName())
                            .zipcode(activity.getZipcode())
                            .interests(activity.getInterests())
                            .page(activity.getPage())
                            .searchTerm(searchTerm)
                            .activityTime(activity.getActivityTime())
                            .build();
                },
                joinWindow,
                StreamJoined.with(Serdes.Integer(), userActivitySerde, searchSerde)
        );

        userActivityStream.peek((userId, activity) ->
                log.info("[STREAM-ENRICHED-OUT] UserID={} User='{}' Zip='{}' Search='{}' Clicked='{}'",
                        userId, activity.getUserName(), activity.getZipcode(), activity.getSearchTerm(), activity.getPage())
        );

        // 6. Sink enriched results to output topic
        userActivityStream.to(
                userActivityTopic,
                Produced.with(Serdes.Integer(), userActivitySerde)
        );
    }
}
