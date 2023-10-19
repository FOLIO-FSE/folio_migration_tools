import json
from pathlib import Path


def deep_merge(target_dict, source_dict, merge_keys=("name", "fileName", "file_name")):
    # deep_merge
    #
    # Deeply merges nested dictionaries and lists of dictionaries.
    # **Muatates the target_dict with the changes**, and returns it.
    #
    # For lists, attempts to match items based on the first found key in merge_keys in each item.
    # If no key is found, or no match, the item is appended to the target list.
    #
    # Deletes any keys with value Null in the source.

    for k in source_dict:
        if isinstance(target_dict.get(k, None), dict) and isinstance(source_dict[k], dict):
            # Recursive, depth-first merge on dictionaries
            target_dict[k] = deep_merge(target_dict[k], source_dict[k], merge_keys)
        elif isinstance(target_dict.get(k, None), list) and isinstance(source_dict[k], list):
            # Merge lists on keys in merge_keys
            for merging_list_item in source_dict[k]:
                if not isinstance(merging_list_item, dict):
                    target_dict[k].append(merging_list_item)
                    continue
                merge_key = next((i for i in merge_keys if i in merging_list_item), None)
                if merge_key is None:
                    target_dict[k].append(merging_list_item)
                    continue
                for index, target in enumerate(target_dict[k]):
                    if target[merge_key] == merging_list_item[merge_key]:
                        target_dict[k][index] = deep_merge(
                            target_dict[k][index],
                            merging_list_item,
                            merge_keys,
                        )
                        break
                else:
                    target_dict[k].append(merging_list_item)
        else:
            if source_dict[k] is None:
                target_dict.pop(k, None)
            else:
                target_dict[k] = source_dict[k]
    return target_dict


def merge_load(config_path_str, parsed_config=None):
    # Recursively load JSON files from a configuration file
    #
    # If a configuration file has a "source" key, either a string or list,
    # the file(s) will be loaded in the order presented, and the config file
    # will be merged on top.
    #
    # To delete a key, set it to `null` in the json source file.

    parsed_config = parsed_config or {}
    config_path = Path(config_path_str)
    with open(config_path) as config_file:
        single_config = json.load(config_file)
    sources = single_config.get("source", [])
    if isinstance(sources, str):
        sources = [sources]
    for source in sources:
        parsed_config = merge_load(config_path.parent / source, parsed_config)
    return deep_merge(parsed_config, single_config)
