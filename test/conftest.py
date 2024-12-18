pytest_plugins = ["test.helpers.auth", "test.helpers.database"]


def pytest_collection_modifyitems(items, config):
    """Modifies test items in place to ensure test classes run in a given order"""
    test_order = [
        "test_index",
        "test_create_source",
        "test_create_organ_sample",
        "test_create_block_sample",
        "test_create_section_sample",
        "test_create_dataset",
    ]

    sorted_items = []
    for test in test_order:
        sorted_items += [item for item in items if item.name == test]

    items[:] = sorted_items
