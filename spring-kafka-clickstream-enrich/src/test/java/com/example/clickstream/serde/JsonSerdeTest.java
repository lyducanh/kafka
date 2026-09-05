package com.example.clickstream.serde;

import com.example.clickstream.model.PageView;
import com.example.clickstream.model.Search;
import com.example.clickstream.model.UserActivity;
import com.example.clickstream.model.UserProfile;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonSerdeTest {

    @Test
    void testUserProfileSerialization() {
        JsonSerde<UserProfile> serde = new JsonSerde<>(UserProfile.class);
        UserProfile profile = UserProfile.builder()
                .userId(1)
                .userName("Matthias")
                .zipcode("94301")
                .interests(List.of("Surfing", "Hiking"))
                .build();

        byte[] bytes = serde.serialize("topic", profile);
        assertNotNull(bytes);

        UserProfile deserialized = serde.deserialize("topic", bytes);
        assertEquals(profile.getUserId(), deserialized.getUserId());
        assertEquals(profile.getUserName(), deserialized.getUserName());
        assertEquals(profile.getZipcode(), deserialized.getZipcode());
        assertEquals(profile.getInterests(), deserialized.getInterests());
    }

    @Test
    void testPageViewSerialization() {
        JsonSerde<PageView> serde = new JsonSerde<>(PageView.class);
        PageView view = new PageView(2, "products/shoes/nike");

        byte[] bytes = serde.serialize("topic", view);
        assertNotNull(bytes);

        PageView deserialized = serde.deserialize("topic", bytes);
        assertEquals(2, deserialized.getUserId());
        assertEquals("products/shoes/nike", deserialized.getPage());
        assertNotNull(deserialized.getTimestamp());
    }

    @Test
    void testSearchSerialization() {
        JsonSerde<Search> serde = new JsonSerde<>(Search.class);
        Search search = new Search(2, "light jacket");

        byte[] bytes = serde.serialize("topic", search);
        assertNotNull(bytes);

        Search deserialized = serde.deserialize("topic", bytes);
        assertEquals(2, deserialized.getUserId());
        assertEquals("light jacket", deserialized.getSearchTerms());
    }

    @Test
    void testUserActivitySerialization() {
        JsonSerde<UserActivity> serde = new JsonSerde<>(UserActivity.class);
        UserActivity activity = UserActivity.builder()
                .userId(1)
                .userName("Matthias")
                .zipcode("94301")
                .interests(List.of("Surfing"))
                .searchTerm("retro wetsuit")
                .page("products/wetsuit")
                .build();

        byte[] bytes = serde.serialize("topic", activity);
        assertNotNull(bytes);

        UserActivity deserialized = serde.deserialize("topic", bytes);
        assertEquals(activity.getUserId(), deserialized.getUserId());
        assertEquals(activity.getUserName(), deserialized.getUserName());
        assertEquals(activity.getSearchTerm(), deserialized.getSearchTerm());
        assertEquals(activity.getPage(), deserialized.getPage());
    }
}
