package com.filemigration.reconcile;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins down this transform's output for every whitespace and case shape it
 * can see, against expected values matching the vendor mock's own
 * extractText. Nothing in this suite reads ocr.js itself, so this cannot
 * catch that file changing on its own; what it does catch is this Java
 * implementation drifting away from the behavior it is supposed to
 * mirror, which would otherwise make the reconciler flag a perfectly good
 * migration as corrupt.
 *
 * Every whitespace code point outside plain ASCII below is written as an
 * explicit unicode escape (backslash, u, four hex digits) rather than a
 * literal character: this source file is plain ASCII throughout, and
 * NBSP, BOM, NEL, and the line/paragraph separators are produced by the
 * Java compiler's own unicode escape handling (JLS 3.3), not by whatever
 * an editor or file encoding happened to preserve.
 */
class OcrTextTransformTest {

    private static final String NBSP = "\u00A0";
    private static final String BOM = "\uFEFF";
    private static final String LINE_SEPARATOR = "\u2028";
    private static final String PARAGRAPH_SEPARATOR = "\u2029";
    private static final String IDEOGRAPHIC_SPACE = "\u3000";
    private static final String NEL = "\u0085";

    @Test
    void collapsesMultipleSpacesToOne() {
        assertEquals("HELLO WORLD", extract("hello   world"));
    }

    @Test
    void collapsesTabsToASingleSpace() {
        assertEquals("HELLO WORLD", extract("hello\t\tworld"));
    }

    @Test
    void collapsesNewlinesToASingleSpace() {
        assertEquals("HELLO WORLD", extract("hello\n\nworld"));
    }

    @Test
    void trimsLeadingAndTrailingWhitespace() {
        assertEquals("HELLO WORLD", extract("   hello world   "));
    }

    @Test
    void upperCasesMixedCaseInput() {
        assertEquals("HELLO WORLD", extract("HeLLo WoRLd"));
    }

    @Test
    void collapsesMixedWhitespaceRunsOfDifferentKinds() {
        assertEquals("HELLO WORLD", extract("hello \t\n \r world"));
    }

    @Test
    void combinesAllCasesAtOnce() {
        assertEquals("INVOICE NUMBER 42 DUE SOON", extract("  Invoice\t Number\n\n42   due\r\nsoon  "));
    }

    @Test
    void emptyInputProducesAnEmptyString() {
        assertEquals("", extract(""));
    }

    @Test
    void wholeStringOfWhitespaceProducesAnEmptyString() {
        assertEquals("", extract("   \t\n  "));
    }

    @Test
    void collapsesAndStripsNonBreakingSpace() {
        // U+00A0: JavaScript's \s matches it, Java's \s and String.trim() do not.
        assertEquals("HELLO WORLD", extract(NBSP + "hello" + NBSP + NBSP + "world" + NBSP));
    }

    @Test
    void collapsesAndStripsByteOrderMark() {
        // U+FEFF: JavaScript's \s matches it, Java's \s and String.trim() do not.
        assertEquals("HELLO", extract(BOM + "hello"));
    }

    @Test
    void collapsesAndStripsUnicodeLineAndParagraphSeparators() {
        assertEquals("HELLO WORLD", extract("hello" + LINE_SEPARATOR + PARAGRAPH_SEPARATOR + "world"));
    }

    @Test
    void collapsesAndStripsIdeographicSpace() {
        // U+3000: part of Unicode's Space_Separator category, matched by
        // JavaScript's \s but not by Java's \s.
        assertEquals("HELLO WORLD", extract(IDEOGRAPHIC_SPACE + "hello" + IDEOGRAPHIC_SPACE + "world"
                + IDEOGRAPHIC_SPACE));
    }

    @Test
    void allNonBreakingSpaceInputProducesAnEmptyString() {
        assertEquals("", extract(NBSP + NBSP + NBSP));
    }

    @Test
    void allByteOrderMarkInputProducesAnEmptyString() {
        assertEquals("", extract(BOM));
    }

    @Test
    void nonAsciiTextIsUpperCasedAndPreservedAcrossMultiByteCharacters() {
        assertEquals("CAF\u00C9 \u00DCBER \u65E5\u672C\u8A9E", extract("caf\u00E9  \u00FCber\t\u65E5\u672C\u8A9E"));
    }

    /**
     * Reproduces the reviewer's byte-level check directly: input codepoints
     * [97,98,99,32,133] ("abc" + space + NEL). Java's regex $ anchor treats
     * U+0085 (NEL) as a line terminator and would let a naive trailing-strip
     * match and remove the space just before it; JavaScript's \s and trim()
     * do not treat NEL as whitespace at all, so the space immediately
     * before it is never reached, let alone stripped. \z instead of $ is
     * what keeps this transform agreeing with that.
     */
    @Test
    void trailingSpaceBeforeANextLineCharacterIsNotStrippedMatchingJavaScript() {
        assertEquals("ABC " + NEL, extract("abc" + " " + NEL));
    }

    private static String extract(String text) {
        return OcrTextTransform.extractText(text.getBytes(StandardCharsets.UTF_8));
    }
}
