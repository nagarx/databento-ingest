"""Tests for downloader helper functions: format_duration, _parse_expected_hash, DownloadProgress."""

from databento_ingest.downloader import (
    DownloadProgress,
    _parse_expected_hash,
    format_duration,
)


class TestFormatDuration:
    def test_seconds(self):
        assert format_duration(12) == "12s", f"Expected '12s', got '{format_duration(12)}'"

    def test_zero(self):
        assert format_duration(0) == "0s", f"Expected '0s', got '{format_duration(0)}'"

    def test_minutes(self):
        assert format_duration(90) == "1m 30s", f"Expected '1m 30s', got '{format_duration(90)}'"

    def test_exact_minute(self):
        assert format_duration(60) == "1m 00s", f"Expected '1m 00s', got '{format_duration(60)}'"

    def test_hours(self):
        assert format_duration(3700) == "1h 01m", f"Expected '1h 01m', got '{format_duration(3700)}'"

    def test_exact_hour(self):
        assert format_duration(3600) == "1h 00m", f"Expected '1h 00m', got '{format_duration(3600)}'"

    def test_large_duration(self):
        result = format_duration(7200 + 900)
        assert result == "2h 15m", f"Expected '2h 15m' for 8100s, got '{result}'"

    def test_negative(self):
        assert format_duration(-1) == "???", f"Expected '???' for negative, got '{format_duration(-1)}'"

    def test_nan(self):
        assert format_duration(float("nan")) == "???", f"Expected '???' for NaN, got '{format_duration(float('nan'))}'"

    def test_fractional_seconds(self):
        assert format_duration(45.7) == "45s", f"Expected '45s' for 45.7, got '{format_duration(45.7)}'"

    def test_fractional_minutes(self):
        assert format_duration(125.9) == "2m 05s", f"Expected '2m 05s' for 125.9, got '{format_duration(125.9)}'"


class TestParseExpectedHash:
    def test_sha256(self):
        algo, digest = _parse_expected_hash("sha256:abcdef1234567890")
        assert algo == "sha256", f"Expected algo='sha256', got '{algo}'"
        assert digest == "abcdef1234567890", f"Expected digest='abcdef1234567890', got '{digest}'"

    def test_uppercase_algo(self):
        algo, digest = _parse_expected_hash("SHA256:abcdef")
        assert algo == "sha256", f"Expected lowered algo='sha256', got '{algo}'"
        assert digest == "abcdef", f"Expected digest='abcdef', got '{digest}'"

    def test_md5(self):
        algo, digest = _parse_expected_hash("md5:d41d8cd98f00b204")
        assert algo == "md5", f"Expected algo='md5', got '{algo}'"
        assert digest == "d41d8cd98f00b204", f"Expected digest='d41d8cd98f00b204', got '{digest}'"

    def test_missing_colon_raises(self):
        try:
            _parse_expected_hash("sha256abcdef")
            assert False, "Should have raised ValueError for missing colon"
        except ValueError as e:
            assert "missing ':'" in str(e), f"Expected 'missing colon' in error, got: {e}"

    def test_colon_in_digest(self):
        algo, digest = _parse_expected_hash("sha256:abc:def")
        assert algo == "sha256", f"Expected algo='sha256', got '{algo}'"
        assert digest == "abc:def", f"Expected digest='abc:def', got '{digest}'"

    def test_empty_digest(self):
        algo, digest = _parse_expected_hash("sha256:")
        assert algo == "sha256", f"Expected algo='sha256', got '{algo}'"
        assert digest == "", f"Expected empty digest, got '{digest}'"


class TestDownloadProgress:
    """Overall-progress accounting must never report 100% before the transfer is
    actually complete. Regression guard for the '[Overall] hits 100% at ~halfway'
    bug: completed-file bytes were counted twice (once per-chunk via add_chunk into
    new_bytes, then again as completed_bytes), so completed_bytes + new_bytes hit
    total_bytes (capped at 100%) at roughly the midpoint of the download.
    """

    def test_completed_file_not_double_counted(self):
        # 2 files x 100 bytes (total 200). File A fully transferred + completed;
        # File B 50% in flight. Correct overall progress = 150/200 = 75%, NOT 100%.
        p = DownloadProgress(total_files=2, total_bytes=200)
        p.add_chunk(100)        # File A streamed
        p.file_done(100)        # File A completed
        p.add_chunk(50)         # File B 50% in flight
        line = p.summary_line()
        assert "(75%)" in line, f"Expected 75% (150/200) mid-download, got: {line}"
        assert "(100%)" not in line, (
            "Progress reported 100% before the download finished — completed-file "
            f"bytes were double-counted: {line}"
        )

    def test_reaches_100_only_when_all_transferred(self):
        # Both files fully transferred + completed -> exactly 100%.
        p = DownloadProgress(total_files=2, total_bytes=200)
        p.add_chunk(100)
        p.file_done(100)
        p.add_chunk(100)
        p.file_done(100)
        line = p.summary_line()
        assert "(100%)" in line, f"Expected 100% once all bytes transferred, got: {line}"

    def test_retry_undo_does_not_inflate_progress(self):
        # A failed attempt's bytes are undone via reset_file_progress, so a partial
        # then-retried file must not push progress past its true transferred amount.
        p = DownloadProgress(total_files=1, total_bytes=100)
        p.add_chunk(40)                 # partial attempt
        p.reset_file_progress(40)       # attempt failed -> undo
        p.add_chunk(25)                 # retry, 25% in flight
        line = p.summary_line()
        assert "(25%)" in line, f"Expected 25% after retry-undo, got: {line}"
        assert "(65%)" not in line, f"Retry bytes not undone (40+25 leaked): {line}"
