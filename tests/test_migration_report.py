from dateutil import parser


def test_time_diff():
    start = parser.parse("2022-06-29T20:21:22")
    end = parser.parse("2022-06-30T21:22:23")
    nice_diff = str(end - start)
    assert nice_diff == "1 day, 1:01:01"
