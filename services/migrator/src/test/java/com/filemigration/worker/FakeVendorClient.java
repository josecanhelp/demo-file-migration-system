package com.filemigration.worker;

import com.filemigration.model.FileRecord;
import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.OcrResult;
import com.filemigration.vendor.VendorClient;
import com.filemigration.vendor.VendorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In-memory stand-in for VendorClient. Counts calls and remembers the ids
 * it was asked to process, so a test can assert exactly what was and was
 * not sent to the vendor. By default it returns a deterministic OCR
 * result per file. A test can arrange for the very next call to throw, or
 * can mark specific ids as poison or as omitted from every result, which
 * stay in effect across calls so an isolation retry can be exercised.
 */
class FakeVendorClient extends VendorClient {

    private final List<List<Long>> calls = new ArrayList<>();
    private final Set<Long> poisonIds = new HashSet<>();
    private final Set<Long> omittedIds = new HashSet<>();
    private VendorException nextException;

    FakeVendorClient() {
        super(null, null);
    }

    int callCount() {
        return calls.size();
    }

    List<Long> idsFromCall(int index) {
        return calls.get(index);
    }

    void throwOnNextCall(VendorException exception) {
        this.nextException = exception;
    }

    /**
     * Any call whose file list includes this id fails PERMANENT, mirroring
     * the real vendor's all-or-nothing rejection of a batch containing one
     * unprocessable document.
     */
    void markPoison(long id) {
        poisonIds.add(id);
    }

    /**
     * The result for this id is left out of every future response,
     * mirroring a vendor response that came back short of what was asked
     * for.
     */
    void omitFromResults(long id) {
        omittedIds.add(id);
    }

    @Override
    public Map<Long, OcrResult> ocrBatch(List<FileRecord> files) {
        List<Long> ids = files.stream().map(FileRecord::id).toList();
        calls.add(ids);
        if (nextException != null) {
            VendorException toThrow = nextException;
            nextException = null;
            throw toThrow;
        }
        if (ids.stream().anyMatch(poisonIds::contains)) {
            throw new VendorException(ErrorClass.PERMANENT, null, "unprocessable document in batch");
        }
        Map<Long, OcrResult> results = new HashMap<>();
        for (FileRecord file : files) {
            if (omittedIds.contains(file.id())) {
                continue;
            }
            results.put(file.id(), new OcrResult(file.id(), "TEXT-" + file.id(), 0.99, 1, "job-" + file.id()));
        }
        return results;
    }
}
