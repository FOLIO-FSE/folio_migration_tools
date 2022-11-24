import logging
from typing import Set

from folio_uuid import FOLIONamespaces
from folioclient import FolioClient
from pymarc import Field
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs


class HRIDHandler:
    def __init__(
        self,
        folio_client: FolioClient,
        handling: HridHandling,
        migration_report: MigrationReport,
        deactivate035_from001: bool,
    ):
        self.unique_001s: Set[str] = set()
        self.deactivate035_from001: bool = deactivate035_from001
        hrid_path = "/hrid-settings-storage/hrid-settings"
        self.folio_client: FolioClient = folio_client
        self.handling: HridHandling = handling
        self.migration_report: MigrationReport = migration_report
        self.hrid_settings = self.folio_client.folio_get_single_object(hrid_path)
        self.instance_hrid_prefix = self.hrid_settings["instances"]["prefix"]
        self.instance_hrid_counter = self.hrid_settings["instances"]["startNumber"]
        self.holdings_hrid_prefix = self.hrid_settings["holdings"]["prefix"]
        self.holdings_hrid_counter = self.hrid_settings["holdings"]["startNumber"]
        self.items_hrid_prefix = self.hrid_settings["items"]["prefix"]
        self.items_hrid_counter = self.hrid_settings["items"]["startNumber"]
        self.common_retain_leading_zeroes: bool = self.hrid_settings["commonRetainLeadingZeroes"]
        logging.info(f"HRID handling is set to: '{self.handling}'")

    def handle_hrid(
        self,
        namespace: FOLIONamespaces,
        folio_record: dict,
        marc_record: Record,
        legacy_ids: list[str],
    ) -> None:
        """Create HRID if not mapped. Add hrid as MARC record 001

        Args:
            namespace (FOLIONamespaces): determening the type of hrid setting to update
            folio_record (dict): _description_
            marc_record (Record): _description_
            legacy_ids (list[str]): _description_

        Raises:
            TransformationProcessError: _description_
        """
        if self.enumerate_hrid(marc_record):
            self.generate_enumerated_hrid(folio_record, marc_record, legacy_ids, namespace)
        elif self.handling == HridHandling.preserve001:
            self.preserve_001_as_hrid(folio_record, marc_record, legacy_ids, namespace)
        else:
            raise TransformationProcessError("", f"Unknown HRID handling: {self.handling}")

    def generate_enumerated_hrid(
        self,
        folio_record: dict,
        marc_record: Record,
        legacy_ids: list[str],
        namespace: FOLIONamespaces,
    ):
        folio_record["hrid"] = self.get_next_hrid(namespace)
        new_001 = Field(tag="001", data=folio_record["hrid"])
        self.handle_035_generation(
            marc_record, legacy_ids, self.migration_report, self.deactivate035_from001
        )
        marc_record.add_ordered_field(new_001)
        self.migration_report.add(Blurbs.HridHandling, "Created HRID using default settings")

    def enumerate_hrid(self, marc_record):
        return self.handling == HridHandling.default or "001" not in marc_record

    def get_next_hrid(self, namespace: FOLIONamespaces):
        hrid = ""
        if namespace == FOLIONamespaces.instances:
            hrid = (
                f"{self.instance_hrid_prefix}"
                f"{self.generate_numeric_part(self.instance_hrid_counter)}"
            )
            self.instance_hrid_counter += 1
        elif namespace == FOLIONamespaces.holdings:
            hrid = (
                f"{self.holdings_hrid_prefix}"
                f"{self.generate_numeric_part(self.holdings_hrid_counter)}"
            )
            self.holdings_hrid_counter += 1
        else:
            raise TransformationProcessError("", "Unimplemented namespace")
        return hrid

    def generate_numeric_part(self, counter):
        return str(counter).zfill(11) if self.common_retain_leading_zeroes else str(counter)

    @staticmethod
    def handle_035_generation(
        marc_record: Record,
        legacy_ids,
        migration_report: MigrationReport,
        deactivate035_from001: bool,
        remove_001: bool = True,
    ):
        try:
            f_001 = marc_record["001"].value()
            f_003 = marc_record["003"].value().strip() if "003" in marc_record else ""
            migration_report.add(Blurbs.HridHandling, f'Values in 003: {f_003 or "Empty"}')

            if deactivate035_from001:
                migration_report.add(Blurbs.HridHandling, "035 generation from 001 turned off")
            else:
                str_035 = f"({f_003}){f_001}" if f_003 else f"{f_001}"
                new_035 = Field(
                    tag="035",
                    indicators=[" ", " "],
                    subfields=["a", str_035],
                )
                marc_record.add_ordered_field(new_035)
                migration_report.add(Blurbs.HridHandling, "Added 035 from 001")
            if remove_001:
                marc_record.remove_fields("001")

        except Exception:
            if "001" in marc_record:
                s = "Failed to create 035 from 001"
                migration_report.add(Blurbs.HridHandling, s)
                Helper.log_data_issue(legacy_ids, s, marc_record["001"])
            else:
                migration_report.add(Blurbs.HridHandling, "Legacy bib records without 001")

    def preserve_001_as_hrid(
        self,
        folio_record: dict,
        marc_record: Record,
        legacy_ids: list[str],
        namespace: FOLIONamespaces,
    ):
        value = marc_record["001"].value()
        if value in self.unique_001s:
            self.migration_report.add(
                Blurbs.HridHandling,
                "Duplicate 001. Creating HRID instead. "
                "Previous 001 will be stored in a new 035 field",
            )
            self.handle_035_generation(
                marc_record, legacy_ids, self.migration_report, self.deactivate035_from001
            )
            Helper.log_data_issue(
                legacy_ids,
                "Duplicate 001 for record. HRID created for record",
                value,
            )
            folio_record["hrid"] = self.get_next_hrid(namespace)
            new_001 = Field(tag="001", data=folio_record["hrid"])
            marc_record.add_ordered_field(new_001)
            self.instance_hrid_counter += 1
        else:
            self.unique_001s.add(value)
            folio_record["hrid"] = value
            self.migration_report.add(Blurbs.HridHandling, "Took HRID from 001")
