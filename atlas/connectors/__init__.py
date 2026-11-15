"""Connector registry.

The pipeline looks connectors up by source_id and never imports one
directly. Adding a source means adding a module and a line here.
"""
from __future__ import annotations

from .base import Connector, ConnectorSpec, Resolver
from .geoboundaries import GeoBoundariesConnector
from .open_meteo import OpenMeteoConnector
from .worldbank import WorldBankConnector

CONNECTORS: dict[str, type[Connector]] = {
    WorldBankConnector.spec.source_id: WorldBankConnector,
    OpenMeteoConnector.spec.source_id: OpenMeteoConnector,
    GeoBoundariesConnector.spec.source_id: GeoBoundariesConnector,
}


def get_connector(source_id: str, storage, client=None) -> Connector:
    try:
        cls = CONNECTORS[source_id]
    except KeyError:
        raise KeyError(
            f"no connector registered for {source_id!r}; "
            f"known: {sorted(CONNECTORS)}"
        ) from None
    return cls(storage=storage, client=client)


__all__ = [
    "CONNECTORS",
    "Connector",
    "ConnectorSpec",
    "GeoBoundariesConnector",
    "OpenMeteoConnector",
    "Resolver",
    "WorldBankConnector",
    "get_connector",
]
