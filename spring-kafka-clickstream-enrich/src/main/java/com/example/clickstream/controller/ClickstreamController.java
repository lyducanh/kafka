package com.example.clickstream.controller;

import com.example.clickstream.listener.UserActivityListener;
import com.example.clickstream.model.PageView;
import com.example.clickstream.model.Search;
import com.example.clickstream.model.UserActivity;
import com.example.clickstream.model.UserProfile;
import com.example.clickstream.service.ClickstreamGeneratorService;
import com.example.clickstream.topology.ClickstreamEnrichmentTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/clickstream")
public class ClickstreamController {

    private final ClickstreamGeneratorService generatorService;
    private final UserActivityListener activityListener;
    private final StreamsBuilderFactoryBean streamsFactoryBean;

    public ClickstreamController(
            ClickstreamGeneratorService generatorService,
            UserActivityListener activityListener,
            StreamsBuilderFactoryBean streamsFactoryBean
    ) {
        this.generatorService = generatorService;
        this.activityListener = activityListener;
        this.streamsFactoryBean = streamsFactoryBean;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        String streamsState = (kafkaStreams != null) ? kafkaStreams.state().name() : "NOT_INITIALIZED";

        return ResponseEntity.ok(Map.of(
                "generatorRunning", generatorService.isRunning(),
                "streamsState", streamsState,
                "recentActivitiesCount", activityListener.getRecentActivities().size()
        ));
    }

    @PostMapping("/sample")
    public ResponseEntity<Map<String, Object>> triggerSampleScenario() {
        Map<String, Object> result = generatorService.runSampleScenario();
        return ResponseEntity.ok(result);
    }

    @PostMapping("/generator/start")
    public ResponseEntity<Map<String, Object>> startGenerator() {
        generatorService.start();
        return ResponseEntity.ok(Map.of("message", "Generator started", "running", true));
    }

    @PostMapping("/generator/stop")
    public ResponseEntity<Map<String, Object>> stopGenerator() {
        generatorService.stop();
        return ResponseEntity.ok(Map.of("message", "Generator stopped", "running", false));
    }

    @PostMapping("/generator/toggle")
    public ResponseEntity<Map<String, Object>> toggleGenerator() {
        boolean isRunning = generatorService.toggle();
        return ResponseEntity.ok(Map.of("message", isRunning ? "Started" : "Stopped", "running", isRunning));
    }

    @PostMapping("/profile")
    public ResponseEntity<?> sendProfile(@RequestBody UserProfile profile) {
        if (profile.getUserName() == null || profile.getUserName().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "userName is required"));
        }
        generatorService.sendProfile(profile);
        return ResponseEntity.ok(Map.of("message", "User profile sent", "profile", profile));
    }

    @PostMapping("/search")
    public ResponseEntity<?> sendSearch(@RequestBody Search search) {
        if (search.getSearchTerms() == null || search.getSearchTerms().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "searchTerms is required"));
        }
        generatorService.sendSearch(search);
        return ResponseEntity.ok(Map.of("message", "Search query sent", "search", search));
    }

    @PostMapping("/pageview")
    public ResponseEntity<?> sendPageView(@RequestBody PageView pageView) {
        if (pageView.getPage() == null || pageView.getPage().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "page is required"));
        }
        generatorService.sendPageView(pageView);
        return ResponseEntity.ok(Map.of("message", "Page view sent", "pageView", pageView));
    }

    @GetMapping("/activities")
    public ResponseEntity<List<UserActivity>> getRecentActivities() {
        return ResponseEntity.ok(activityListener.getRecentActivities());
    }

    /**
     * Interactive Query: Query all user profiles directly from the local RocksDB KTable state store.
     */
    @GetMapping("/store/profiles")
    public ResponseEntity<?> queryAllProfilesFromStore() {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyKeyValueStore<Integer, UserProfile> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            ClickstreamEnrichmentTopology.USER_PROFILE_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            List<Map<String, Object>> profiles = new ArrayList<>();
            try (KeyValueIterator<Integer, UserProfile> iterator = store.all()) {
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("userId", entry.key);
                    item.put("profile", entry.value);
                    profiles.add(item);
                }
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", ClickstreamEnrichmentTopology.USER_PROFILE_STORE,
                    "storageEngine", "RocksDB",
                    "totalProfilesInStore", profiles.size(),
                    "profiles", profiles
            ));
        } catch (Exception e) {
            log.error("Error querying RocksDB profile store", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Interactive Query: Query a specific user profile by ID from the local RocksDB state store.
     */
    @GetMapping("/store/profiles/{userId}")
    public ResponseEntity<?> queryProfileByIdFromStore(@PathVariable int userId) {
        KafkaStreams kafkaStreams = streamsFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            return ResponseEntity.status(503).body(Map.of("error", "Kafka Streams is not running yet"));
        }

        try {
            ReadOnlyKeyValueStore<Integer, UserProfile> store = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(
                            ClickstreamEnrichmentTopology.USER_PROFILE_STORE,
                            QueryableStoreTypes.keyValueStore()
                    )
            );

            UserProfile profile = store.get(userId);
            if (profile == null) {
                return ResponseEntity.status(404).body(Map.of("error", "Profile not found in store for userId=" + userId));
            }

            return ResponseEntity.ok(Map.of(
                    "storeName", ClickstreamEnrichmentTopology.USER_PROFILE_STORE,
                    "userId", userId,
                    "profile", profile
            ));
        } catch (Exception e) {
            log.error("Error querying RocksDB profile store for userId=" + userId, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
