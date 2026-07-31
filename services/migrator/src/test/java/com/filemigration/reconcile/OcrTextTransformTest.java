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
 */
class OcrTextTransformTest {

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

    private static String extract(String text) {
        return OcrTextTransform.extractText(text.getBytes(StandardCharsets.UTF_8));
    }
}
