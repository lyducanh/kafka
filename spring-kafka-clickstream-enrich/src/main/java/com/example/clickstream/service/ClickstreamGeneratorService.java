package com.example.clickstream.service;

import com.example.clickstream.model.PageView;
import com.example.clickstream.model.Search;
import com.example.clickstream.model.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class ClickstreamGeneratorService {

    @Value("${app.kafka.topics.user-profile:clicks.user.profile}")
    private String userProfileTopic;

    @Value("${app.kafka.topics.page-views:clicks.pages.views}")
    private String pageViewsTopic;

    @Value("${app.kafka.topics.search:clicks.search}")
    private String searchTopic;

    private final KafkaTemplate<Integer, Object> kafkaTemplate;
    private final AtomicBoolean running;
    private final Random random = new Random();

    private final Map<Integer, UserProfile> userDirectory = new HashMap<>();

    private static final String[][] MOCK_ACTIVITIES = {
            {"running shoes", "products/shoes/nike-air-zoom", "products/shoes/brooks-ghost-15"},
            {"wireless headphones", "products/audio/sony-wh1000xm5", "products/audio/bose-qc45"},
            {"ergonomic chair", "products/office/herman-miller-aeron", "products/office/steelcase-gesture"},
            {"mechanical keyboard", "products/tech/keychron-q1-pro", "products/tech/ducky-one-3"},
            {"espresso machine", "products/kitchen/breville-barista-express", "products/kitchen/de-longhi-dedica"},
            {"camping tent", "products/outdoor/mSR-hubba-hubba", "products/outdoor/big-agnes-copper-spur"}
    };

    public ClickstreamGeneratorService(
            KafkaTemplate<Integer, Object> kafkaTemplate,
            @Value("${app.generator.enabled:false}") boolean initialEnabled
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.running = new AtomicBoolean(initialEnabled);

        // Pre-seed some default user profiles
        userDirectory.put(1, new UserProfile(1, "Matthias", "94301", new String[]{"Surfing", "Hiking"}));
        userDirectory.put(2, new UserProfile(2, "Anna", "94302", new String[]{"Ski", "Dancing"}));
        userDirectory.put(3, new UserProfile(3, "Bob", "10001", new String[]{"Gaming", "Coffee"}));
        userDirectory.put(4, new UserProfile(4, "Claire", "98101", new String[]{"Photography", "Travel"}));
        userDirectory.put(5, new UserProfile(5, "David", "60601", new String[]{"Cycling", "Cooking"}));
    }

    public boolean isRunning() {
        return running.get();
    }

    public void start() {
        running.set(true);
        log.info("Clickstream background simulator STARTED");
    }

    public void stop() {
        running.set(false);
        log.info("Clickstream background simulator STOPPED");
    }

    public boolean toggle() {
        boolean newState = !running.get();
        running.set(newState);
        log.info("Clickstream simulator toggled: {}", newState ? "RUNNING" : "STOPPED");
        return newState;
    }

    public void sendProfile(UserProfile profile) {
        userDirectory.put(profile.getUserId(), profile);
        kafkaTemplate.send(userProfileTopic, profile.getUserId(), profile).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send profile for userId={}: {}", profile.getUserId(), ex.getMessage());
            } else {
                log.info("[PRODUCED-PROFILE] UserID={} Name='{}' Zip='{}' Interests={}",
                        profile.getUserId(), profile.getUserName(), profile.getZipcode(), profile.getInterests());
            }
        });
    }

    public void sendSearch(Search search) {
        kafkaTemplate.send(searchTopic, search.getUserId(), search).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send search for userId={}: {}", search.getUserId(), ex.getMessage());
            } else {
                log.info("[PRODUCED-SEARCH] UserID={} Query='{}'", search.getUserId(), search.getSearchTerms());
            }
        });
    }

    public void sendPageView(PageView pageView) {
        kafkaTemplate.send(pageViewsTopic, pageView.getUserId(), pageView).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send pageView for userId={}: {}", pageView.getUserId(), ex.getMessage());
            } else {
                log.info("[PRODUCED-PAGEVIEW] UserID={} Page='{}'", pageView.getUserId(), pageView.getPage());
            }
        });
    }

    /**
     * Executes the exact demo scenario from Gwen Shapira's GenerateData class.
     */
    public Map<String, Object> runSampleScenario() {
        log.info("Starting Gwen Shapira Clickstream Sample Scenario...");

        // 1. Two profiles
        UserProfile user1 = new UserProfile(1, "Matthias", "94301", new String[]{"Surfing", "Hiking"});
        UserProfile user2 = new UserProfile(2, "Anna", "94302", new String[]{"Ski", "Dancing"});
        sendProfile(user1);
        sendProfile(user2);

        // 2. Profile update for Anna
        UserProfile user2Updated = user2.update("94303", new String[]{"Ski", "stream processing"});
        sendProfile(user2Updated);

        // 3. Two searches
        Search search1 = new Search(1, "retro wetsuit");
        Search search2 = new Search(2, "light jacket");
        sendSearch(search1);
        sendSearch(search2);

        // 4. Three clicks
        PageView view1 = new PageView(1, "collections/mens-wetsuits/products/w3-worlds-warmest-wetsuit");
        PageView view2 = new PageView(2, "product/womens-dirt-craft-bike-mountain-biking-jacket");
        PageView view3 = new PageView(2, "/product/womens-ultralight-down-jacket");
        sendPageView(view1);
        sendPageView(view2);
        sendPageView(view3);

        // 5. Subsequent session searches and clicks
        Search search3 = new Search(2, "carbon ski boots");
        sendSearch(search3);

        PageView view4 = new PageView(2, "product/salomon-quest-access-custom-heat-ski-boots-womens");
        PageView view5 = new PageView(2, "product/nordica-nxt-75-ski-boots-womens");
        sendPageView(view4);
        sendPageView(view5);

        // 6. Anonymous user click without search
        PageView view6 = new PageView(-1, "product/osprey-atmos-65-ag-pack");
        sendPageView(view6);

        log.info("Gwen Shapira Clickstream Sample Scenario completed.");
        return Map.of(
                "status", "SUCCESS",
                "profilesSent", 3,
                "searchesSent", 3,
                "pageViewsSent", 6,
                "message", "Sample events produced to clicks.* topics successfully."
        );
    }

    @Scheduled(fixedDelayString = "${app.generator.interval-ms:3000}")
    public void generateRandomActivity() {
        if (!running.get()) {
            return;
        }

        // Pick user (1-5 or -1 for guest)
        int userChoice = random.nextInt(6);
        int userId = (userChoice == 0) ? -1 : userChoice;

        // Ensure user profile exists in Kafka if not guest
        if (userId > 0 && random.nextInt(5) == 0) {
            UserProfile p = userDirectory.get(userId);
            if (p != null) {
                sendProfile(p);
            }
        }

        // Pick random activity template
        String[] activity = MOCK_ACTIVITIES[random.nextInt(MOCK_ACTIVITIES.length)];
        String query = activity[0];
        String page = activity[random.nextInt(activity.length - 1) + 1];

        // 70% chance search first, then click within window
        if (userId > 0 && random.nextDouble() < 0.75) {
            sendSearch(new Search(userId, query));
        }

        sendPageView(new PageView(userId, page));
    }
}
