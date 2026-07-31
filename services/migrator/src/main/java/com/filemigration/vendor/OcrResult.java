package com.filemigration.vendor;

/**
 * The OCR output the vendor returns for a single document: the extracted
 * text, the vendor's confidence in it, how many pages it spans, and the
 * vendor's own job id for that document.
 */
public record OcrResult(long id, String text, double confidence, int pageCount, String jobId) {
}
