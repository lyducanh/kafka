package com.example.clickstream.listener;

import com.example.clickstream.model.UserActivity;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class UserActivityListener {

    private final List<UserActivity> recentActivities = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_HISTORY = 50;

    @KafkaListener(
            topics = "${app.kafka.topics.user-activity:clicks.user.activity}",
            groupId = "${spring.kafka.consumer.group-id:clickstream-enrich-consumer-group}",
            properties = {
                    "spring.json.use.type.headers=false",
                    "spring.json.value.default.type=com.example.clickstream.model.UserActivity"
            }
    )
    public void listen(ConsumerRecord<Integer, UserActivity> record) {
        UserActivity activity = record.value();
        log.info("[LISTENER-RECEIVED] Key={} UserID={} Name='{}' Zip='{}' Search='{}' Clicked='{}'",
                record.key(), activity.getUserId(), activity.getUserName(),
                activity.getZipcode(), activity.getSearchTerm(), activity.getPage());

        recentActivities.add(0, activity);
        if (recentActivities.size() > MAX_HISTORY) {
            recentActivities.remove(recentActivities.size() - 1);
        }
    }

    public List<UserActivity> getRecentActivities() {
        return new ArrayList<>(recentActivities);
    }
}
