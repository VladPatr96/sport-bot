from work.categorization import normalize_tag_name

def test_normalize_tag_name_basic():
    assert normalize_tag_name("  Спартак   Москва ") == "спартак москва"
