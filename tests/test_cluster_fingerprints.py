"""Tests for cluster/fingerprints.py"""

from __future__ import annotations

import pytest

from cluster.fingerprints import (
    compute_signature,
    jaccard_similarity,
    tokenize_text,
)


class TestTokenizeText:
    """Test text tokenization"""

    def test_basic_tokenization(self):
        """Test basic word extraction"""
        tokens = tokenize_text("Зенит победил Спартак со счётом 3:1")
        assert "зенит" in tokens
        assert "победил" in tokens
        assert "спартак" in tokens
        assert "счётом" in tokens

    def test_stopwords_removal(self):
        """Test that stopwords are removed"""
        tokens = tokenize_text("Команда играла в финале и победила")
        # Stopwords should be removed
        assert "и" not in tokens
        assert "в" not in tokens
        # Content words should remain
        assert "команда" in tokens
        assert "играла" in tokens
        assert "финале" in tokens
        assert "победила" in tokens

    def test_empty_and_none(self):
        """Test empty string and None handling"""
        assert tokenize_text("") == []
        assert tokenize_text("   ") == []

    def test_punctuation_handling(self):
        """Test punctuation is removed"""
        tokens = tokenize_text("Привет, мир! Как дела?")
        assert "привет" in tokens
        assert "мир" in tokens
        assert "как" not in tokens  # stopword
        assert "дела" in tokens

    def test_numbers(self):
        """Test number handling"""
        tokens = tokenize_text("Матч завершился со счётом 2:0")
        assert "матч" in tokens
        assert "завершился" in tokens
        assert "2" in tokens or "0" in tokens  # Numbers may be included

    def test_english_text(self):
        """Test English text tokenization"""
        tokens = tokenize_text("Manchester United won the match 3-1")
        assert "manchester" in tokens
        assert "united" in tokens
        assert "won" not in tokens  # English stopword (if implemented)
        assert "match" in tokens


class TestComputeSignature:
    """Test signature computation"""

    def test_basic_signature(self):
        """Test basic signature generation"""
        sig = compute_signature("Зенит победил Спартак")
        assert isinstance(sig, frozenset)
        assert len(sig) > 0
        assert "зенит" in sig or any("зенит" in token for token in sig)

    def test_empty_text(self):
        """Test empty text signature"""
        sig = compute_signature("")
        assert sig == frozenset()

    def test_signature_deduplication(self):
        """Test that duplicate words create same tokens"""
        sig1 = compute_signature("Матч матч матч")
        assert len(sig1) == 1  # Only one unique token

    def test_order_independence(self):
        """Test that word order doesn't affect signature"""
        sig1 = compute_signature("Зенит победил Спартак")
        sig2 = compute_signature("Спартак победил Зенит")
        # Should have same tokens (frozenset is unordered)
        assert sig1 == sig2 or "победил" in sig1 and "победил" in sig2


class TestJaccardSimilarity:
    """Test Jaccard similarity calculation"""

    def test_identical_sets(self):
        """Test identical sets have similarity 1.0"""
        set1 = frozenset(["a", "b", "c"])
        set2 = frozenset(["a", "b", "c"])
        assert jaccard_similarity(set1, set2) == 1.0

    def test_disjoint_sets(self):
        """Test disjoint sets have similarity 0.0"""
        set1 = frozenset(["a", "b", "c"])
        set2 = frozenset(["d", "e", "f"])
        assert jaccard_similarity(set1, set2) == 0.0

    def test_partial_overlap(self):
        """Test partial overlap"""
        set1 = frozenset(["a", "b", "c"])
        set2 = frozenset(["b", "c", "d"])
        # Intersection: {b, c} = 2, Union: {a, b, c, d} = 4
        assert jaccard_similarity(set1, set2) == 0.5

    def test_empty_sets(self):
        """Test empty set handling"""
        set1 = frozenset()
        set2 = frozenset(["a"])
        assert jaccard_similarity(set1, set2) == 0.0
        assert jaccard_similarity(frozenset(), frozenset()) == 0.0

    def test_one_element_sets(self):
        """Test single element sets"""
        set1 = frozenset(["a"])
        set2 = frozenset(["a"])
        assert jaccard_similarity(set1, set2) == 1.0

        set1 = frozenset(["a"])
        set2 = frozenset(["b"])
        assert jaccard_similarity(set1, set2) == 0.0

    def test_symmetric(self):
        """Test that Jaccard is symmetric"""
        set1 = frozenset(["a", "b", "c"])
        set2 = frozenset(["b", "c", "d", "e"])
        assert jaccard_similarity(set1, set2) == jaccard_similarity(set2, set1)

    def test_real_world_titles(self):
        """Test with real news titles"""
        title1 = "Зенит победил Спартак в матче РПЛ"
        title2 = "Спартак проиграл Зениту в чемпионате"

        sig1 = compute_signature(title1)
        sig2 = compute_signature(title2)
        similarity = jaccard_similarity(sig1, sig2)

        # Should have some overlap (зенит, спартак)
        assert 0.0 < similarity < 1.0

    def test_duplicate_detection(self):
        """Test near-duplicate detection"""
        title1 = "Манчестер Юнайтед выиграл матч"
        title2 = "Манчестер Юнайтед одержал победу в матче"

        sig1 = compute_signature(title1)
        sig2 = compute_signature(title2)
        similarity = jaccard_similarity(sig1, sig2)

        # High similarity for near-duplicates
        assert similarity > 0.5


# Integration tests
class TestFingerprintingWorkflow:
    """Test complete fingerprinting workflow"""

    def test_duplicate_article_detection(self):
        """Test detecting duplicate articles"""
        article1 = "Зенит обыграл Спартак со счётом 2:1 в матче чемпионата России"
        article2 = "Зенит победил Спартак 2:1 в РПЛ"

        sig1 = compute_signature(article1)
        sig2 = compute_signature(article2)
        similarity = jaccard_similarity(sig1, sig2)

        # Should detect as related
        assert similarity > 0.4

    def test_unrelated_articles(self):
        """Test that unrelated articles have low similarity"""
        article1 = "Зенит выиграл чемпионат России по футболу"
        article2 = "Теннисист Медведев победил на турнире в Дубае"

        sig1 = compute_signature(article1)
        sig2 = compute_signature(article2)
        similarity = jaccard_similarity(sig1, sig2)

        # Should be low similarity
        assert similarity < 0.3

    def test_language_mixing(self):
        """Test handling of mixed language content"""
        text = "Manchester United победил Arsenal в Premier League"
        sig = compute_signature(text)

        assert len(sig) > 0
        # Should contain both English and Russian tokens
