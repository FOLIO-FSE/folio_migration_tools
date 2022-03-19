import pytest

from migration_tools.custom_exceptions import TransformationProcessError
from migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)


def test_holdings_notes():
    folio_rec = {"notes": [{"note": "apa", "holdingsNoteTypeId": ""}]}
    with pytest.raises(TransformationProcessError):
        HoldingsCsvTransformer.handle_notes(folio_rec)


def test_holdings_notes2():
    folio_rec = {"notes": [{"note": "", "holdingsNoteTypeId": "apa"}]}
    HoldingsCsvTransformer.handle_notes(folio_rec)
    assert folio_rec["notes"] == []
