"""Tests for downloader helper functions: format_duration, _parse_expected_hash."""

from databento_ingest.downloader import _parse_expected_hash, format_duration


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
