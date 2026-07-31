package com.filemigration.reconcile;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Recomputes the same text transform the vendor mock applies to a document's
 * bytes: decode as UTF-8, uppercase, collapse every run of whitespace to a
 * single space, then strip leading and trailing whitespace. Kept in exact
 * lockstep with that transform so the reconciler can check a stored
 * document.ocr_text value by recomputing the expected result from the
 * source blob instead of trusting the stored value on its own; any
 * divergence between this and the vendor's own transform would surface as
 * a mismatch against an otherwise correctly migrated file.
 *
 * "Whitespace" here is defined to match what JavaScript's regex \s matches,
 * not what Java's \s or String.trim() match: beyond the ASCII space and
 * control characters both languages agree on, JavaScript's \s also matches
 * U+00A0 (non-breaking space), U+FEFF (byte order mark / zero-width
 * no-break space), U+1680, U+2000 through U+200A, U+2028, U+2029, U+202F,
 * U+205F, and U+3000, none of which Java's \s or String.trim() treat as
 * whitespace at all. A source file with a leading BOM or an embedded NBSP
 * would otherwise produce a value here that disagrees with the vendor
 * mock's own extractText for an otherwise perfectly migrated file.
 *
 * The leading and trailing patterns below anchor with \A and \z, the true
 * start and end of input, rather than ^ and $: without MULTILINE those
 * still normally coincide, except that Java's $ additionally matches just
 * before a trailing line terminator, and Java's definition of line
 * terminator includes U+0085 (NEL), U+2028, and U+2029, none of which
 * JavaScript's \s treats as whitespace at all (\s and $ are two separate
 * questions in JavaScript, and neither answers the other the way Java's $
 * does here). A source file ending in ordinary whitespace immediately
 * followed by a NEL would otherwise have that whitespace stripped here but
 * not by the vendor mock's own trim(), producing a mismatch on an
 * otherwise perfectly migrated file.
 */
public final class OcrTextTransform {

    private static final String JS_WHITESPACE_CLASS =
            "[\\t\\n\\u000B\\f\\r\\u0020\\u00A0\\uFEFF\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000]";
    private static final Pattern WHITESPACE_RUN = Pattern.compile(JS_WHITESPACE_CLASS + "+");
    private static final Pattern LEADING_WHITESPACE = Pattern.compile("\\A" + JS_WHITESPACE_CLASS + "+");
    private static final Pattern TRAILING_WHITESPACE = Pattern.compile(JS_WHITESPACE_CLASS + "+\\z");

    private OcrTextTransform() {
    }

    public static String extractText(byte[] content) {
        String decoded = new String(content, StandardCharsets.UTF_8);
        String upper = decoded.toUpperCase(Locale.ROOT);
        String collapsed = WHITESPACE_RUN.matcher(upper).replaceAll(" ");
        return strip(collapsed);
    }

    /**
     * Strips leading and trailing runs matched by JS_WHITESPACE_CLASS.
     * Deliberately not String.trim(), which only strips characters at or
     * below U+0020 and would leave a boundary NBSP or BOM behind.
     */
    private static String strip(String value) {
        String withoutLeading = LEADING_WHITESPACE.matcher(value).replaceFirst("");
        return TRAILING_WHITESPACE.matcher(withoutLeading).replaceFirst("");
    }
}
