package com.filemigration.worker;

import com.filemigration.model.FileRecord;
import com.filemigration.vendor.OcrResult;
import com.filemigration.vendor.VendorClient;
import com.filemigration.vendor.VendorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory stand-in for VendorClient. Counts calls and remembers the ids
 * it was asked to process, so a test can assert exactly what was and was
 * not sent to the vendor. By default it returns a deterministic OCR
 * result per file; a test can instead arrange for the next call to throw.
 */
class FakeVendorClient extends VendorClient {

    private final List<List<Long>> calls = new ArrayList<>();
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

    @Override
    public Map<Long, OcrResult> ocrBatch(List<FileRecord> files) {
        List<Long> ids = files.stream().map(FileRecord::id).toList();
        calls.add(ids);
        if (nextException != null) {
            VendorException toThrow = nextException;
            nextException = null;
            throw toThrow;
        }
        Map<Long, OcrResult> results = new HashMap<>();
        for (FileRecord file : files) {
            results.put(file.id(), new OcrResult(file.id(), "TEXT-" + file.id(), 0.99, 1, "job-" + file.id()));
        }
        return results;
    }
}
