import datetime
import json
from uuid import uuid4

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient
from pymarc.reader import MARCReader
from pymarc.record import Field
from pymarc.record import Record

from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)

# flake8: noqa: E501


def test_dedupe_recs():
    my_dict = {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    RulesMapperBase.dedupe_rec(my_dict)
    assert my_dict != {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    assert my_dict == {"my_arr": [{"a": "b"}, {"c": "d"}]}


def test_datetime_from_005():
    f005_1 = "19940223151047.0"
    record = Record()
    record.add_field(Field(tag="005", data=f005_1))
    instance = {
        "metadata": {
            "createdDate": datetime.datetime.utcnow().isoformat(),
            "updatedDate": datetime.datetime.utcnow().isoformat(),
        }
    }
    RulesMapperBase.set_005_as_updated_date(record, instance, "some_id")
    assert instance["metadata"]["updatedDate"] == "1994-02-23T15:10:47"


def test_date_from_008():
    f008 = "170309s2017\\\\quc\\\\\o\\\\\000\0\fre\d"
    record = Record()
    record.add_field(Field(tag="008", data=f008))
    instance = {
        "title": "some title",
        "metadata": {
            "createdDate": datetime.datetime.utcnow().isoformat(),
            "updatedDate": datetime.datetime.utcnow().isoformat(),
        },
    }
    RulesMapperBase.use_008_for_dates(record, instance, "some_id")
    assert instance["catalogedDate"] == "2017-03-09"
    # assert instance["metadata"]["createdDate"] == "2017-03-09T00:00:00"


def test_remove_subfields():
    marc_field = Field(
        tag="338",
        indicators=["0", "1"],
        subfields=[
            "b",
            "ac",
            "b",
            "ab",
            "i",
            "ba",
        ],
    )
    new_field = RulesMapperBase.remove_repeated_subfields(marc_field)
    assert len(new_field.subfields_as_dict()) == len(marc_field.subfields_as_dict())
    assert len(marc_field.subfields) == 6
    assert len(new_field.subfields) == 4


def test_date_from_008_holding():
    f008 = "170309s2017\\\\quc\\\\\o\\\\\000\0\fre\d"
    record = Record()
    record.add_field(Field(tag="008", data=f008))
    holding = {
        "metadata": {
            "createdDate": datetime.datetime.utcnow().isoformat(),
            "updatedDate": datetime.datetime.utcnow().isoformat(),
        }
    }
    RulesMapperBase.use_008_for_dates(record, holding, "some_id")
    assert "catalogedDate" not in holding
    # assert holding["metadata"]["createdDate"] == "2017-03-09T00:00:00"


def test_add_entity_to_record():
    entity = {"id": "id", "type": "type"}
    rec = {}
    latest_schema = FolioClient.get_latest_from_github(
        "folio-org", "mod-inventory-storage", "ramls/instance.json"
    )
    RulesMapperBase.add_entity_to_record(entity, "identifiers", rec, latest_schema)
    assert rec == {"identifiers": [{"id": "id", "type": "type"}]}


def test_weirdness():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
        for record in reader:
            record1 = record
        f020s = record1.get_fields("020")
        mapping = {"subfield": ["a"]}
        subfields = f020s[1].get_subfields(*mapping["subfield"])
        assert subfields


def test_grouped():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
        for record in reader:
            record1 = record
        f020s = record1.get_fields("020")
        grouped = RulesMapperBase.grouped(f020s[1])
        for tf in grouped:
            assert type(tf) is Field
            assert tf.tag == "020"
            assert tf.subfields in [
                ["a", "0870990004 (v. 1)", "c", "20sek"],
                ["a", "0870990020 (v. 2)", "c", "20sek"],
            ]

        for field in f020s:
            grouped = RulesMapperBase.grouped(field)
            for tf in grouped:
                mapping = {"subfield": ["a"]}
                subfields = tf.get_subfields(*mapping["subfield"])
                assert subfields


def test_get_srs_string_bib():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        instance = {"id": str(uuid4()), "hrid": "my hrid"}
        metadata = {}
        id_holder = {
            "instanceId": instance["id"],
            "instanceHrid": instance["hrid"],
        }
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
        for record in reader:
            record1 = record
            srs_record_string = RulesMapperBase.get_srs_string(
                record1,
                instance,
                str(uuid4()),
                metadata,
                True,
                FOLIONamespaces.instances,
            )
            assert '"recordType": "MARC_BIB"' in srs_record_string
            assert json.dumps(id_holder) in srs_record_string
            assert "snapshotId" not in record


def test_get_srs_string_bad_leaders():
    path = "./tests/test_data/corrupt_leader.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        l1 = record.leader
        record.leader = f"{record.leader[:-4]}4500"
        assert l1 != record.leader
        assert record.leader.endswith("4500")
        assert len(record.leader) == 24


def test_create_srs_uuid():
    created_id = RulesMapperBase.create_srs_id(FOLIONamespaces.holdings, "some_url", "id_1")
    assert str(created_id) == "6734f228-cba2-54c7-b129-c6437375a864"
    created_id_2 = RulesMapperBase.create_srs_id(FOLIONamespaces.instances, "some_url", "id_1")
    assert str(created_id) != str(created_id_2)
