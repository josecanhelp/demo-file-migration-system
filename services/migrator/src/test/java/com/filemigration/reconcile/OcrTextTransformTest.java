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
 * Every whitespace code point outside plain ASCII is written as an
 * explicit unicode escape rather than a literal character, so the
 * invisible or easily-mangled ones (NBSP, BOM, line/paragraph separator)
 * are unambiguous in source rather than relying on an editor or file
 * encoding to preserve them correctly.
 */
class OcrTextTransformTest {

    private static final String NBSP = " ";
    private static final String BOM = "﻿";
    private static final String LINE_SEPARATOR = " ";
    private static final String PARAGRAPH_SEPARATOR = " ";
    private static final String IDEOGRAPHIC_SPACE = "　";

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
        assertEquals("CAFÉ ÜBER 日本語", extract("café  über\t日本語"));
    }

    private static String extract(String text) {
        return OcrTextTransform.extractText(text.getBytes(StandardCharsets.UTF_8));
    }
}
