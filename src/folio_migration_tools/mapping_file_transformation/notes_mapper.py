import logging
import i18n

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)


class NotesMapper(MappingFileMapperBase):
    def __init__(
        self,
        library_configuration: LibraryConfiguration,
        folio_client: FolioClient,
        record_map: dict,
        object_type: FOLIONamespaces,
        ignore_legacy_identifier: bool = False,
    ) -> None:
        self.folio_client: FolioClient = folio_client
        self.setup_notes_schema()
        super().__init__(
            folio_client,
            self.notes_schema,
            record_map,
            None,
            object_type,
            library_configuration,
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
                        FOLIONamespaces.organizations: "organization",
                        FOLIONamespaces.orders: "orders",
                    }.get(record_type)
                    note["links"] = [{"id": object_uuid, "type": type_string}]
                    if "type" in note:
                        del note["type"]
                    self.extradata_writer.write("notes", note)
                    self.migration_report.add_general_statistics(
                        i18n.t("Number of linked notes created")
                    )
                    self.migration_report.add("MappedNoteTypes", note["typeId"])
                else:
                    self.migration_report.add_general_statistics(
                        i18n.t("Number of discarded notes with no content")
                    )

    def get_notes_schema(self):
        notes_schema = self.folio_client.get_from_github(
            "folio-org",
            "mod-notes",
            "src/main/resources/swagger.api/schemas/note.yaml",
        )
        notes_common = self.folio_client.get_from_github(
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
