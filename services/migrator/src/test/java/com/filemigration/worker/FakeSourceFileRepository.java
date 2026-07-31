package com.filemigration.worker;

import com.filemigration.model.FileRecord;
import com.filemigration.store.SourceFileRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory stand-in for SourceFileRepository. Records are looked up by
 * id and simply omitted from the result when not present, matching the
 * real repository's behavior for ids that no longer exist.
 */
class FakeSourceFileRepository extends SourceFileRepository {

    private final Map<Long, FileRecord> records = new HashMap<>();

    FakeSourceFileRepository() {
        super(null);
    }

    void put(FileRecord record) {
        records.put(record.id(), record);
    }

    @Override
    public List<FileRecord> findByIds(List<Long> ids) {
        List<FileRecord> found = new ArrayList<>();
        for (Long id : ids) {
            FileRecord record = records.get(id);
            if (record != null) {
                found.add(record);
            }
        }
        return found;
    }
}
