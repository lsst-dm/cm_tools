from typing import Iterable

import numpy as np
from lsst.daf.butler import Butler, CollectionType


def get_sorted_array(itr: Iterable, field: str) -> np.ndarray:
    the_set = set()
    for x_ in itr:
        the_set.add(x_.dataId[field])
    the_array = np.array([x_ for x_ in the_set])
    return np.sort(the_array)


def print_dataset_summary(stream, butler_url: str, collections: list[str]) -> None:
    butler = Butler(butler_url, collections=collections)

    summary_dict = {}
    config_dict = {}
    schema_dict = {}
    metadata_dict = {}
    log_dict = {}
    dict_of_dicts = dict(
        summary=summary_dict,
        config=config_dict,
        schema=schema_dict,
        metadata=metadata_dict,
        log=log_dict,
    )
    for results in butler.registry.queryDatasets(...).byParentDatasetType():
        n_dataset = results.count(exact=False)
        ds_name = results.parentDatasetType.name
        if n_dataset == 0:
            continue
        if ds_name.find("_config") > 0:
            which_dict = config_dict
        elif ds_name.find("_schema") > 0:
            which_dict = schema_dict
        elif ds_name.find("_metadata") > 0:
            which_dict = metadata_dict
        elif ds_name.find("_log") > 0:
            which_dict = log_dict
        else:
            which_dict = summary_dict
        which_dict[results.parentDatasetType.name] = (n_dataset, results.parentDatasetType)
    for dict_name, a_dict in sorted(dict_of_dicts.items()):
        stream.write(f"{dict_name} --------------\n")
        for ds_name, (n_ds, dt) in sorted(a_dict.items()):
            stream.write(f"{ds_name:60} {n_ds:10} {str(dt)}\n")
        stream.write("\n")
    stream.flush()


def build_data_queries(
    butler: Butler,
    dataset: str,
    field: str,
    min_queries: int = 1,
    max_step_size: int = 10000000,
) -> list[str]:

    itr = butler.registry.queryDatasets(dataset)
    sorted_field_values = get_sorted_array(itr, field)

    n_matched = sorted_field_values.size

    step_size = min(max_step_size, int(n_matched / min_queries))

    ret_list = []

    previous_idx = 0
    idx = 0

    while idx < n_matched:
        idx += step_size
        max_idx = min(idx, n_matched - 1)
        min_val = sorted_field_values[previous_idx]
        max_val = sorted_field_values[max_idx]
        ret_list.append(f"({min_val} <= {field}) and ({field} < {max_val})")
        previous_idx = idx

    return ret_list


def clean_collection_set(
    butler: Butler,
    input_colls: list[list[str]],
) -> list[list[str]]:

    output_colls = []
    for input_colls_ in input_colls:
        existing_colls = []
        for input_coll_ in input_colls_:
            colls = butler.registry.queryCollections(input_coll_)
            try:
                count = 0
                for cc in colls:
                    count += 1
            except Exception:
                count = 0
            if count:
                existing_colls.append(input_coll_)
        output_colls.append(existing_colls)
    return output_colls


def butler_associate_kludge(
    butler_repo: str,
    output_coll: str,
    input_colls: list[list[str]],
) -> None:

    assert input_colls

    butler = Butler(butler_repo, writeable=True)
    input_colls = clean_collection_set(butler, input_colls)
    first_colls = input_colls[0]
    output_coll = output_coll + "_test"
    tagged_coll = output_coll + "_tagged"

    butler.registry.registerCollection(tagged_coll, CollectionType.TAGGED)
    for input_colls_ in input_colls[1:]:
        refset = butler.registry.queryDatasets(
            ...,
            collections=input_colls_,
            findFirst=True,
        ).byParentDatasetType()
        for refs in refset:
            if refs.parentDatasetType.dimensions:
                butler.registry.associate(tagged_coll, refs)
    butler.registry.registerCollection(output_coll, CollectionType.CHAINED)
    all_cols = first_colls + [tagged_coll]
    butler.registry.setCollectionChain(output_coll, all_cols)
