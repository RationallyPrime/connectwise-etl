from __future__ import annotations

from .extract import extract
from .transform import process_fabric_data


def extract_to_fabric(entity: str, **params):  # noqa: D401
    """Extract *entity* from ConnectWise and land in OneLake.

    Parameters
    ----------
    entity : str
        One of the keys registered in *fabric_api.extract.REGISTRY*.
    **params : Any
        Query‑string parameters forwarded to the underlying API call.

    Returns
    -------
    pandas.DataFrame
        The materialised data frame that was also persisted to OneLake.
    """
    rows = extract(entity, **params)
    _, df = process_fabric_data(rows, entity)
    return df
