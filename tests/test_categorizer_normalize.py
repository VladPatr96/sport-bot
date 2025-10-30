"""Tests for categorizer/normalize.py"""

from __future__ import annotations

import pytest

from categorizer.normalize import normalize_token


class TestNormalizeToken:
    """Test normalize_token function"""

    def test_basic_normalization(self):
        """Test basic string normalization"""
        assert normalize_token("Hello World") == "hello world"
        assert normalize_token("  SPACES  ") == "spaces"
        assert normalize_token("TeSt") == "test"

    def test_none_and_empty(self):
        """Test None and empty string handling"""
        assert normalize_token(None) == ""
        assert normalize_token("") == ""
        assert normalize_token("   ") == ""

    def test_special_characters(self):
        """Test special character replacement"""
        assert normalize_token("hello-world") == "hello world"
        assert normalize_token("foo_bar_baz") == "foo bar baz"
        assert normalize_token("test--double") == "test double"

    def test_boundary_characters(self):
        """Test removal of boundary non-word characters"""
        assert normalize_token("!hello!") == "hello"
        assert normalize_token("...test...") == "test"
        assert normalize_token("@#$%word&*()") == "word"
        assert normalize_token("!!!") == ""

    def test_multiple_whitespace(self):
        """Test multiple whitespace consolidation"""
        assert normalize_token("hello    world") == "hello world"
        assert normalize_token("a  b  c") == "a b c"
        assert normalize_token("test\t\nword") == "test word"

    def test_unicode_preservation(self):
        """Test that unicode letters are preserved"""
        assert normalize_token("Москва") == "москва"
        assert normalize_token("Спартак-Москва") == "спартак москва"
        assert normalize_token("Barça") == "barça"
        assert normalize_token("Münch") == "münch"

    def test_numbers(self):
        """Test number handling"""
        assert normalize_token("Team 123") == "team 123"
        assert normalize_token("2024-Season") == "2024 season"

    def test_real_world_examples(self):
        """Test real-world team/player names"""
        assert normalize_token("ФК Зенит") == "фк зенит"
        assert normalize_token("Manchester-United") == "manchester united"
        assert normalize_token("Cristiano_Ronaldo") == "cristiano ronaldo"
        assert normalize_token("  Lionel Messi  ") == "lionel messi"
        assert normalize_token("FC-Barcelona") == "fc barcelona"

    def test_edge_cases(self):
        """Test edge cases"""
        assert normalize_token("A") == "a"
        assert normalize_token("1") == "1"
        assert normalize_token("-") == ""
        assert normalize_token("_") == ""
        assert normalize_token("!!!Hello!!!World!!!") == "hello world"
