from folio_migration_tools.config_file_load import deep_merge, merge_load


def test_single_file_config_load():
    loaded_text = merge_load("./tests/test_data/config_files/single_file_config.json")
    assert loaded_text == {"a": 1, "b": [1, 2, 3], "c": {"d": 4, "e": 5}}


def test_config_dict_merge():
    loaded_text = merge_load("./tests/test_data/config_files/config_dict_merge.json")
    assert loaded_text == {
        "a": 2,
        "b": [1, 2, 3, 4],
        "source": "single_file_config.json",
        "c": {"d": 4, "e": 5},
    }


def test_config_list_item_merge():
    loaded_text = merge_load("./tests/test_data/config_files/config_list_item_merge_source.json")
    assert loaded_text == {
        "a": [
            {
                "name": 1,
                "value1": 2,
                "value2": 3,
                "value3": 4,
            },
            {"name": 2, "value1": 21, "value2": 22},
            {"name": 3, "value2": 32, "value3": 23},
        ],
        "source": "config_list_item_merge_target.json",
    }


def test_config_attribute_clearing():
    pass


def test_config_chained_load():
    loaded_text = merge_load("./tests/test_data/config_files/config_dict_chain.json")
    assert loaded_text == {
        "a": 3,
        "b": [1, 2, 3, 4, 5],
        "source": "config_dict_merge.json",
        "c": {"d": 4, "e": 5},
    }


def test_merge_order():
    loaded_text = merge_load("./tests/test_data/config_files/config_merge_order_source.json")
    assert loaded_text == {
        "a": 5,
        "b": [1, 2, 3, 4, 5],
        "source": ["config_dict_merge.json", "config_merge_order_target.json"],
        "c": 5,
    }


def test_empty_merge():
    d = {"a": 1}
    assert deep_merge({}, {}) == {}
    assert deep_merge(d, {}) == d
    assert deep_merge({}, d) == d


def test_merge_clearing():
    first = {"a": 1, "b": 2}
    assert deep_merge(first, {}) == first
    assert deep_merge(first, {"a": None}) == {"b": 2}


def test_merge_dict_chain():
    assert deep_merge(deep_merge({"a": [1], "b": 2}, {"a": None}), {"a": [2]}) == {
        "a": [2],
        "b": 2,
    }


def test_deep_merging_behavior():
    first = {"a": {"b": {"c": {"d": 1, "e": 2}}}}
    second = {"a": {"b": {"c": {"d": 3}}}}
    assert deep_merge(first, second) == {"a": {"b": {"c": {"d": 3, "e": 2}}}}


def test_deep_merge_list_dicts():
    first = {"a": [{"b": "1", "c": "2"}, {"b": "3", "c": "4"}]}
    second = {"a": [{"b": "1", "c": "3"}, {"b": "2", "c": "3"}]}
    assert deep_merge(first, second) == {
        "a": [
            {"b": "1", "c": "2"},
            {"b": "3", "c": "4"},
            {"b": "1", "c": "3"},
            {"b": "2", "c": "3"},
        ]
    }
    first = {"a": [{"b": "1", "c": "2"}, {"b": "3", "c": "4"}]}
    second = {"a": [{"b": "1", "c": "3"}, {"b": "2", "c": "3"}]}
    assert deep_merge(first, second, ("b")) == {
        "a": [{"b": "1", "c": "3"}, {"b": "3", "c": "4"}, {"b": "2", "c": "3"}]
    }
