from real_estate_scraper.parsing import extract_numeric_value, extract_rooms_and_bedrooms, \
    extract_text_before_and_within_brackets, extract_dutch_postcode_and_city, \
    normalize_city_names

import pytest


def test_extract_numeric_value():
    test_cases = [
        {"string": "300000 k.k.", "expected": 300000.0},
        {"string": "300000,23", "expected": 300000.23, "decimal_delimiter": ',',
         "thousands_delimiter": '.'},
        {"string": "1'000'000", "expected": 1000000.0, "thousands_delimiter": "'"},
        {"string": "100 m2", "expected": 100},
        {"string": "250 sqf", "expected": 250},
        {"string": "120.2 m2", "expected": 120.2},
        {"string": "$ 300'000 k.k.", "expected": 300000.0, "thousands_delimiter":
            "'"},
        {"string": "100,00", "decimal_delimiter": ",", "thousands_delimiter": ",",
         "expected": ValueError},
        {"string": "m2m 30000 k.k.", "expected": 30000.0},
        {"string": "m2m a30000 k.k.", "expected": None},
        {"string": "", "expected": None}
    ]
    for i, test_case in enumerate(test_cases, start=1):
        expected = test_case.pop("expected")
        if expected == ValueError:
            with pytest.raises(ValueError):
                extract_numeric_value(**test_case)
        else:
            try:
                result = extract_numeric_value(**test_case)
                assert result == expected, f"Test case {i} failed: expected " \
                                           f"{expected} but got {result}"
            except AssertionError as e:
                pytest.fail(str(e))


def test_extract_rooms_and_bedrooms():
    test_cases = [
        ("4 rooms (3 bedrooms)", (4, 3)),
        ("6 rooms", (6, None)),
        ("5 rooms (1 bedroom)", (5, 1)),
        ("1 room (1 bedroom)", (1, 1)),
        ("", (None, None)),
        ("3", (None, None))
    ]

    for string, expected_result in test_cases:
        result = extract_rooms_and_bedrooms(string)
        assert result == expected_result, f'For input "{string}", expected ' \
                                          f'"{expected_result}" but got "{result}"'


def test_extract_text_before_and_within_brackets():
    test_cases = [
        ("semi-detached residential property (patio residence)", ("semi-detached "
                                                                  "residential "
                                                                  "property",
                                                                  "patio residence")),
        ("residential property", ("residential property", None)),
        ("  (residential property)", (None, "residential property")),
        ("()", (None, None)),
        (" (", (None, None)),
        (None, (None, None))
    ]

    for string, expected_result in test_cases:
        result = extract_text_before_and_within_brackets(string)
        assert result == expected_result, f'For input "{string}", expected ' \
                                          f'"{expected_result}" but got "{result}"'


def test_extract_dutch_postcode_and_city():
    test_cases = [("3191 XC Hoogvliet Rotterdam", ("3191", "XC", "Hoogvliet Rotterdam")),
                  ("1234 Rotterdam", ("1234", None, "Rotterdam")),
                  (None, None)
                  ]
    for string, expected_result in test_cases:
        result = extract_dutch_postcode_and_city(string)
        assert result == expected_result, f'For input "{string}", expected ' \
                                          f'"{expected_result}" but got "{result}"'


def test_normalize_city_names():
    test_cases = [
        ("New York", "new-york"),
        ("San Francisco", "san-francisco"),
        ("L'Aquila", "l-aquila"),
    ]

    for string, expected in test_cases:
        result = normalize_city_names(string)
        assert result == expected, f'For input "{string}", expected ' \
                                   f'"{expected}" but got "{result}"'
