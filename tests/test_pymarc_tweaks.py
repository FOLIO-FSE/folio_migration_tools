import json

from pymarc import MARCReader


def get_path(file_name):
    return f"./tests/test_data/diacritics/{file_name}"


def test_escapes_with_json():
    with open("./tests/test_data/default/escape_chars.mrc", "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        record = next(reader)
        title = record["245"]["a"]
        assert title == '[HAROLD DAVID "HAL" SANDY AND WILDA BARMORE SANDY COLLECTION].]'

        dumped_title = json.dumps({"title": record.title()})
        assert (
            dumped_title
            == '{"title": "[HAROLD DAVID \\"HAL\\" SANDY AND WILDA BARMORE SANDY COLLECTION].]"}'
        )
        marc_json = record.as_json()
        assert '"Hall"' not in marc_json

        assert '\\"HAL\\"' in marc_json


def test_amharic():
    with open(
        get_path("AmharicTest.mrc"),
        "rb",
    ) as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = False
        for record in reader:
            assert record["880"]["a"] == "የአድዋው ጀግና :"
            assert record["880"]["c"] == "ኤስ ጴጥሮስ ፔትሪዲስ እንደጻፈው."
            assert record["880"]["b"] == "የልዑል ራስ መኮንን ታሪክ /"


#  def test_raw_from_sierra():
#     with open(
#         get_path("diac-4-theo-directly-from-sierra.mrc"),
#         "rb",
#     ) as marc_file:
#         reader = MARCReader(marc_file, to_unicode=True, permissive=True)
#         reader.hide_utf8_warnings = True
#         reader.force_utf8 = False
#         for idx, record in enumerate(reader):
#             assert record.title
#             f_880 = record["880"].format_field()
#             f_a = record["880"]["a"]
#             s = str(f_a).decode("cesu-8").encode("utf-8")
#             print(s)
#             logging.info(s)


# def test_processed_from_sierra():
#     with open(
#         get_path("diac-4-theo-MarcEdit-utf8-processed.mrc"),
#         "rb",
#     ) as marc_file:
#         reader = MARCReader(marc_file, to_unicode=True, permissive=True)
#         reader.hide_utf8_warnings = True
#         reader.force_utf8 = True
#         for idx, record in enumerate(reader):
#             assert record.title
#             f_880 = record["880"].format_field()
#             f_a = record["880"]["a"]
#             logging.info(f_a)
