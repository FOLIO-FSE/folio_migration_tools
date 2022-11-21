import logging

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapping_base_impl import (
    MappingFileMappingBaseImpl,
)
from folio_migration_tools.report_blurbs import Blurbs


class NotesMapper(MappingFileMappingBaseImpl):
    def __init__(
        self,
        library_configuration: LibraryConfiguration,
        folio_client: FolioClient,
        record_map: dict,
        object_type: FOLIONamespaces,
        ignore_legacy_identifier: bool = False,
    ) -> None:
        self.folio_client = folio_client
        self.setup_notes_schema()
        super().__init__(
            library_configuration,
            folio_client,
            self.notes_schema,
            record_map,
            object_type,
            ignore_legacy_identifier,
        )

        self.noteprops = {
            "data": [p for p in record_map["data"] if p["folio_field"].startswith("notes[")]
        }
        logging.info("Set %s props used for note mapping", len(self.noteprops["data"]))
        logging.info("Initiated mapper for Notes")

    def setup_notes_schema(self):
        notes_schemas = self.get_notes_schema()
        self.notes_schema = notes_schemas["noteCollection"]
        self.notes_schema["properties"]["notes"]["items"] = notes_schemas["note"]
        self.notes_schema["required"] = []

    def map_notes(self, legacy_object, legacy_id, object_uuid: str, record_type: FOLIONamespaces):
        if any(self.noteprops["data"]):

            for note in self.do_map(legacy_object, legacy_id, FOLIONamespaces.note)[0].get(
                "notes", []
            ):
                if note.get("content", "").strip():
                    type_string = {
                        FOLIONamespaces.users: "user",
                        FOLIONamespaces.course: "course",
                    }.get(record_type)
                    note["links"] = [{"id": object_uuid, "type": type_string}]
                    del note["type"]
                    self.extradata_writer.write("notes", note)
                    self.migration_report.add(Blurbs.MappedNoteTypes, note["typeId"])
                else:
                    self.migration_report.add_general_statistics(
                        "Notes without content that were discarded. Set some default "
                        "value if you only intend to set the note title"
                    )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])

        if folio_prop_name in self.mapped_from_values and len(legacy_item_keys) == 1:
            return self.mapped_from_values.get(folio_prop_name, "")

        map_entries = list(
            MappingFileMapperBase.get_map_entries_by_folio_prop_name(
                folio_prop_name, self.record_map["data"]
            )
        )
        if len(map_entries) > 1:
            self.migration_report.add(Blurbs.Details, f"{legacy_item_keys} were concatenated")
        return " ".join(
            MappingFileMapperBase.get_legacy_value(legacy_item, map_entry, self.migration_report)
            for map_entry in map_entries
        ).strip()

    @staticmethod
    def get_notes_schema():
        notes_schema = FolioClient.get_latest_from_github(
            "folio-org",
            "mod-notes",
            "src/main/resources/swagger.api/schemas/note.yaml",
        )
        notes_common = FolioClient.get_latest_from_github(
            "folio-org",
            "mod-notes",
            "src/main/resources/swagger.api/schemas/common.yaml",
        )
        for prop in notes_schema["note"]["properties"].items():
            if prop[1].get("$ref", "") == "common.yaml#/uuid":
                prop[1]["type"] = notes_common["uuid"]["type"]

        for p in ["links", "metadata", "id"]:
            del notes_schema["note"]["properties"][p]
        return notes_schema
