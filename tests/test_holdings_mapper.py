from unittest.mock import Mock

from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)


def test_get_call_number_1():
    call_number = "['Merrymount']"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_2():
    call_number = "[English Poetry]WELB"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_3():
    call_number = "PR2754 .F4 [13]"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_4():
    call_number = "PR2754 .Y8 [13]"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_5():
    call_number = "[English Poetry]WELB"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number
