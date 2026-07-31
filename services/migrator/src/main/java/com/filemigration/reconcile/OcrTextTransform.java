package com.filemigration.reconcile;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Recomputes the same text transform the vendor mock applies to a document's
 * bytes: decode as UTF-8, uppercase, collapse every run of whitespace to a
 * single space, then trim. Kept in exact lockstep with that transform so the
 * reconciler can check a stored document.ocr_text value by recomputing the
 * expected result from the source blob instead of trusting the stored value
 * on its own; any divergence between this and the vendor's own transform
 * would surface as a mismatch against an otherwise correctly migrated file.
 */
public final class OcrTextTransform {

    private static final Pattern WHITESPACE_RUN = Pattern.compile("\\s+");

    private OcrTextTransform() {
    }

    public static String extractText(byte[] content) {
        String decoded = new String(content, StandardCharsets.UTF_8);
        String upper = decoded.toUpperCase(Locale.ROOT);
        return WHITESPACE_RUN.matcher(upper).replaceAll(" ").trim();
    }
}
