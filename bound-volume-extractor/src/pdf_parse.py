#!/usr/bin/env python3
"""
Layout-aware text extraction for NY bound-volume PDFs.

Reads a PDF via pdfplumber and emits one JSON record per page on stdout (NDJSON),
each containing:
  - page index (0-based)
  - page width/height
  - extracted text (full page, layout-preserving)
  - lines (list of text runs with their bounding boxes)
  - words (list of words with bounding boxes and font metadata)
  - font occurrences (which fonts appear and how often)

The downstream Node parser uses this raw structure to detect opinion
boundaries, captions, citations, etc. We deliberately do NOT do that
structural parsing here — it's all in JS so the audit/output layer can
inspect intermediate state easily.

Usage: pdf_parse.py <path-to-pdf>
"""

import sys
import json
from collections import Counter

try:
    import pdfplumber
except ImportError:
    print(
        "ERROR: pdfplumber not installed. From this directory:\n"
        "  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(2)


def extract_page(page):
    # x_tolerance=1 (vs pdfplumber's default 3) stops adjacent characters
    # without a space glyph in the PDF stream from being merged into one
    # mega-token. NY official-reports PDFs frequently lack space chars in
    # body text — the typesetter relies on visual gaps — so the default
    # tolerance produced run-together "stormsafflictingthestate" words.
    text = page.extract_text(layout=True, x_tolerance=1) or ""
    raw_text = page.extract_text(x_tolerance=1) or ""

    # Words with bbox + font info — useful for detecting style changes
    # (small caps for parties, bold for headers, italics for citations).
    words = []
    for w in page.extract_words(extra_attrs=["fontname", "size"], x_tolerance=1):
        words.append({
            "text": w["text"],
            "x0": round(w["x0"], 2),
            "x1": round(w["x1"], 2),
            "top": round(w["top"], 2),
            "bottom": round(w["bottom"], 2),
            "fontname": w.get("fontname"),
            "size": round(float(w.get("size", 0)), 2),
        })

    # Group words into lines for easier downstream processing.
    lines = []
    current_top = None
    current_line = []
    for w in sorted(words, key=lambda w: (w["top"], w["x0"])):
        if current_top is None or abs(w["top"] - current_top) <= 2.0:
            current_line.append(w)
            current_top = w["top"] if current_top is None else current_top
        else:
            lines.append(current_line)
            current_line = [w]
            current_top = w["top"]
    if current_line:
        lines.append(current_line)

    # Re-sort words within each line by x0 before joining: the line-grouping
    # loop above iterates words in (top, x0) order, so when a word from a
    # slightly-different top (within tolerance) joins an existing line it
    # gets appended at the end instead of inserted at its correct x position.
    # E.g. lead-cap "F" at top=483.19 lands AFTER body words at top=483.18,
    # producing ", J. (concurring). Experience ... arguably F" instead of
    # "F , J. (concurring). Experience ... arguably". Sorting by x0 fixes
    # this and matters for any byline whose lead-cap renders on a slightly
    # different baseline than the rest of the line.
    #
    # In-body footnote markers are typeset as superscripts at ~5.5pt (vs
    # body 10.98pt). When rendering line text we wrap such words with
    # sentinels — `` start, `` end — so downstream JS can find
    # marker positions in the body and strip them while recording offsets.
    # Sentinels survive normalizeLineWraps (they're not \n / hyphen / space)
    # and the private-use code points won't collide with real opinion text.
    SUPERSCRIPT_SIZE_MAX = 6.0
    SENTINEL_FN_START = ""
    SENTINEL_FN_END = ""
    def _render_word(w):
        if w["size"] < SUPERSCRIPT_SIZE_MAX:
            return f"{SENTINEL_FN_START}{w['text']}{SENTINEL_FN_END}"
        return w["text"]

    line_records = [{
        "top": line[0]["top"],
        "x0": min(w["x0"] for w in line),
        "x1": max(w["x1"] for w in line),
        "text": " ".join(_render_word(w) for w in sorted(line, key=lambda w: w["x0"])),
        "fontname": Counter(w["fontname"] for w in line if w["fontname"]).most_common(1)[0][0]
            if any(w["fontname"] for w in line) else None,
        "size": Counter(w["size"] for w in line).most_common(1)[0][0],
    } for line in lines]

    fonts = Counter(w["fontname"] for w in words if w["fontname"])

    return {
        "page_index": page.page_number - 1,
        "width": round(page.width, 2),
        "height": round(page.height, 2),
        "text_layout": text,
        "text_raw": raw_text,
        "lines": line_records,
        "words": words,
        "fonts": dict(fonts.most_common()),
    }


def main():
    if len(sys.argv) != 2:
        print("usage: pdf_parse.py <pdf-path>", file=sys.stderr)
        sys.exit(2)

    pdf_path = sys.argv[1]

    with pdfplumber.open(pdf_path) as pdf:
        meta = {
            "kind": "meta",
            "page_count": len(pdf.pages),
            "metadata": pdf.metadata or {},
        }
        print(json.dumps(meta, default=str))

        for page in pdf.pages:
            try:
                rec = extract_page(page)
                rec["kind"] = "page"
                print(json.dumps(rec, default=str))
            except Exception as e:
                print(json.dumps({
                    "kind": "page_error",
                    "page_index": page.page_number - 1,
                    "error": str(e),
                }))


if __name__ == "__main__":
    main()
