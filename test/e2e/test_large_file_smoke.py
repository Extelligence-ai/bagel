"""Optional local-only smoke test (#134): a real multi-GB log can be described
and window-queried without exhausting memory. Skips unless the external
fixture is present; never a CI gate.

Point BAGEL_LARGE_FIXTURE at any real large capture (.blf/.asc with a .dbc
beside it, .mf4, or a ros .log directory) to run it.
"""

import os
import pathlib

import pytest

LARGE_FIXTURE = os.environ.get("BAGEL_LARGE_FIXTURE")

pytestmark = pytest.mark.e2e


@pytest.mark.skipif(not LARGE_FIXTURE, reason="BAGEL_LARGE_FIXTURE not set")
def test_describe_and_windowed_query_on_large_file() -> None:
    from src.di import module
    from src.di.types.base_module import BaseModule
    from src.di.types.data_source import resolve

    path = pathlib.Path(LARGE_FIXTURE)
    assert path.exists()
    ds_type = resolve(str(path))
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": str(path)}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    # describe must complete without materializing the file
    assert factory.metadata
    start = factory.start_seconds
    # a 1-second window near the start must be cheap
    relation = dataset.to_duckdb(factory, registry, start_seconds=start, end_seconds=start + 1.0)
    relation.df()
