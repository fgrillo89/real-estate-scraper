from real_estate_scraper.utils import camelcase, split_list


def test_split_list():
    test_cases = [
        ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
        ([1, 2, 3, 4, 5], 3, [[1, 2, 3], [4, 5]]),
        ([1, 2, 3, 4, 5, 6], 2, [[1, 2], [3, 4], [5, 6]]),
        ([], 2, []),
        ([1], 2, [[1]])
    ]
    for input_list, chunksize, expected in test_cases:
        result = split_list(input_list, chunksize)
        assert result == expected, f"For {input_list} and {chunksize}, expected" \
                                   f" {expected} but got {result}"


def test_camelcase():
    test_cases = [
        ("hello world", " ", "HelloWorld"),
        ("hello_world", "_", "HelloWorld"),
        ("HELLO_WORLD", "_", "HelloWorld"),
        ("HELLO WORLD", " ", "HelloWorld"),
        ("", " ", "")
    ]
    for string, separator, expected in test_cases:
        result = camelcase(string, separator)
        assert result == expected, f"For {string} and {separator}, expected {expected} " \
                                   f"but got {result}"
