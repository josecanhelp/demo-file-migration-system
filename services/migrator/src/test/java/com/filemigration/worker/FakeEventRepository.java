package com.filemigration.worker;

import com.filemigration.model.Stage;
import com.filemigration.store.EventRepository;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory stand-in for EventRepository. Keeps every recorded event so a
 * test can check both which stages fired and for which ids.
 */
class FakeEventRepository extends EventRepository {

    private final List<Event> events = new ArrayList<>();

    FakeEventRepository() {
        super(null);
    }

    List<Event> events() {
        return events;
    }

    long countByStage(Stage stage) {
        return events.stream().filter(event -> event.stage() == stage).count();
    }

    long countByStageAndId(Stage stage, long sourceId) {
        return events.stream()
                .filter(event -> event.stage() == stage && Long.valueOf(sourceId).equals(event.sourceId()))
                .count();
    }

    @Override
    public void record(Long sourceId, Stage stage, String lane, String detailJson) {
        events.add(new Event(sourceId, stage, lane, detailJson));
    }

    record Event(Long sourceId, Stage stage, String lane, String detailJson) {
    }
}
