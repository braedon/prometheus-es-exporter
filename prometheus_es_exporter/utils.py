def merge_dicts(*dict_args, **extra_entries):
    """
    Given an arbitrary number of dictionaries, merge them into a
    single new dictionary. Later dictionaries take precedence if
    a key is shared by multiple dictionaries.

    Extra entries can also be provided via kwargs. These entries
    have the highest precedence.
    """
    res = {}

    for d in dict_args + (extra_entries,):
        res.update(d)

    return res
