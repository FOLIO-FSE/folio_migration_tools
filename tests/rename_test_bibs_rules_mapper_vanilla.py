import types
from migration_tools.library_configuration import HridHandling
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
import unittest
from lxml import etree
import pymarc
import json
import re
from types import SimpleNamespace
from collections import namedtuple
from jsonschema import validate
from folioclient.FolioClient import FolioClient
from migration_tools.migration_tasks.bibs_transformer import BibsTransformer


class RulesMapperVanilla(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("./tests/test_config.json") as settings_file:
            cls.config = json.load(
                settings_file,
                object_hook=lambda d: namedtuple("X", d.keys())(*d.values()),
            )
            cls.folio = FolioClient(
                cls.config.okapi_url,
                cls.config.tenant_id,
                cls.config.username,
                cls.config.password,
            )
            conf = BibsTransformer.TaskConfiguration(
                name="test",
                migration_task_type="BibsTransformer",
                hrid_handling=HridHandling.default,
                files=[],
                ils_flavour="sierra",
            )
            args_dict = {"suppress": False, "ils_flavour": "voyager"}
            cls.mapper = BibsRulesMapper(cls.folio, SimpleNamespace(**args_dict), conf)
            cls.instance_schema = cls.folio.get_instance_json_schema()
            print("Done setupclass in test")

    def default_map(self, file_name, xpath):
        ns = {
            "marc": "https://www.loc.gov/MARC21/slim",
            "oai": "https://www.openarchives.org/OAI/2.0/",
        }
        file_path = f"./tests/test_data/default/{file_name}"
        record = pymarc.parse_xml_to_array(file_path)[0]
        (result, other) = self.mapper.parse_bib(["legacy_id"], record, False)
        if self.config.validate_json_schema:
            validate(result, self.instance_schema)
        root = etree.parse(file_path)
        data = str("")
        for element in root.xpath(xpath, namespaces=ns):
            data = " ".join(
                [data, str(etree.tostring(element, pretty_print=True), "utf-8")]
            )
        # print(json.dumps(rec, indent=4, sort_keys=True))
        return [result, data]

    def test_simple_title(self):
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map("test1.xml", xpath)
        self.assertEqual(
            "Modern Electrosynthetic Methods in Organic Chemistry", record[0]["title"]
        )
        # TODO: test abcense of / for chalmers

    def test_multiple336s(self):
        xpath = "//marc:datafield[@tag='336']"
        record = self.default_map("test_multiple_336.xml", xpath)
        self.assertEqual(
            "8105bd44-e7bd-487e-a8f2-b804a361d92f", record[0]["instanceTypeId"]
        )

    def test_strange_isbn(self):
        xpath = "//marc:datafield[@tag='020']"
        record = self.default_map("isbn_c.xml", xpath)
        self.assertTrue(record[0].get("identifiers", None))
        identifiers = list(f["identifierTypeId"] for f in record[0]["identifiers"])
        self.assertTrue(all(identifiers))
        for i in identifiers:
            self.assertEqual(1, len(str.split(i)))

    def test_composed_title(self):
        message = (
            "Should create a composed title (245) with the [a, b, k, n, p] subfields"
        )
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map("test_composed_title.xml", xpath)
        # self.assertFalse('/' in record['title'])
        self.assertEqual(
            "The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment. / Steen Hyldgaard Christensen, Christelle Didier, Andrew Jamison, Martin Meganck, Carl Mitcham, Byron Newberry, editors.",
            record[0]["title"],
            message + "\n" + record[1],
        )

    def test_alternative_titles_246(self):
        message = "Should match 246 to alternativeTitles"
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map("test3.xml", xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = "Engineering identities, epistemologies and values"
        alt_titles = list(
            (t["alternativeTitle"] for t in record[0]["alternativeTitles"])
        )
        self.assertIn(title, alt_titles, message + "\n" + record[1])

    def test_alternative_titles_130(self):
        message = "Should match 130 to alternativeTitles"
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map("test4.xml", xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = "Les cahiers d'urbanisme"
        alt_titles = list(
            (t["alternativeTitle"] for t in record[0]["alternativeTitles"])
        )
        self.assertIn(title, alt_titles, message + "\n" + record[1])

    def alternative_titles_246_and_130(self):
        message = "Should match 246 to alternativeTitles when there is also 130"
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map("test4.xml", xpath)
        title = "Cahiers d'urbanisme et d'aménagement du territoire"
        alt_titles = list(
            (t["alternativeTitle"] for t in record[0]["alternativeTitles"])
        )
        self.assertIn(title, alt_titles, message + "\n" + record[1])

    def alternative_titles_4(self):
        message = "Should match 222 to alternativeTitles when there is also 130"
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map("test4.xml", xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = "Urbana tidskrifter"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )
        title = "Cahiers d'urbanisme et d'aménagement du territoire 57/58/59"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )
        title = "Les cahiers d'urbanisme"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )

    def test_editions(self):
        message = "Should add editions (250) to the editions list and enforce unique"
        xpath = "//marc:datafield[@tag='250']"
        record = self.default_map("test_editions.xml", xpath)
        editions_stmts = ["8. uppl", "[Revised]"]
        for stmt in editions_stmts:
            self.assertIn(stmt, record[0]["editions"], message + "\n" + record[1])

    def test_languages_041(self):
        message = "Should add languages (041$a) to the languages list; ignores non-ISO languages"
        xpath = "//marc:datafield[@tag='041']"
        record = self.default_map("test_multiple_languages.xml", xpath)
        lang_codes = ["eng", "ger", "fre", "ita"]
        should_not_be_there = ["en_US", "###", "zxx"]
        for lang_code in should_not_be_there:
            self.assertNotIn(
                lang_code, record[0]["languages"], message + "\n" + record[1]
            )
        for lang_code in lang_codes:
            self.assertIn(lang_code, record[0]["languages"], message + "\n" + record[1])

    def test_languages_008(self):
        message = "Should add language found in 008 where there is no 041"
        xpath = "//marc:controlfield[@tag='008']"
        record = self.default_map("test_language_in_008.xml", xpath)
        self.assertIn("fre", record[0]["languages"], message + "\n" + record[1])

    def test_physical_descriptions(self):
        message = "Should add physical descriptions (300$abce)"
        xpath = "//marc:datafield[@tag='300']"
        record = self.default_map("test_physical_descriptions.xml", xpath)
        phy_des = "xxxiv, 416 pages illustrations 24 cm."
        self.assertIn(
            phy_des, record[0]["physicalDescriptions"], message + "\n" + record[1]
        )

    def test_index_title(self):
        message = "Should trim title (245) by n-chars, as specified by indicator 2"
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map("test_index_title.xml", xpath)
        self.assertEqual(
            "Cahiers d'urbanisme", record[0]["indexTitle"], message + "\n" + record[1]
        )

    def test_alternative_titles_all(self):
        message = "Should add all types of alternative titles: 130, 222, 240, 246, 247 "
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map("test_alternative_titles.xml", xpath)
        # 246
        title = "Engineering identities, epistemologies and values remainder title"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )
        # 247
        title = "Medical world news annual review of medicine"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )
        # 240
        title = "Laws, etc. (Laws of Kenya : 1948)"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )
        # 130
        title = "Star is born (Motion picture : 1954)"
        self.assertIn(
            title,
            list(t["alternativeTitle"] for t in record[0]["alternativeTitles"]),
            message + "\n" + record[1],
        )

    def test_identifiers(self):
        message = "Should add identifiers: 010, 019, 020, 022, 024, 028, 035"
        xpath = "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']"
        record = self.default_map("test_identifiers.xml", xpath)
        expected_identifiers = [
            "(OCoLC)ocn898162644",
            "19049386",
            "PJC 222013 Paris Jazz Corner Productions",
            "1234-1231",
            "677051564",
            "244170452",
            "62874189",
            "2008011507",
            "a 1",
            "a 2",
            "9780307264787",
            "9780071842013 (paperback) 200 SEK",
            "0071842012 (paperback)",
            "9780307264777",
            "9780307264755",
            "9780307264766",
            "0376-4583",
            "0027-3473 1560-15605 0046-2254",
            "1560-15605 0027-3473 0046-2254",
            "0027-3473 0046-2254 1560-15605",
            "1560-15605 0046-2254 0027-3473",
            "0027-3475",
            "0027-3476",
            "1234-1232",
            "7822183031",
            "M011234564",
            "(OCoLC)898162644",
            "a only",
            "z only",
        ]
        m = message + "\n" + record[1]
        folio_uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
        for id in record[0]["identifiers"]:
            with self.subTest(id["value"]):
                self.assertIn(
                    id["value"],
                    expected_identifiers,
                    f"{json.dumps(id, indent=4)}- {m}",
                )
                self.assertTrue(
                    re.match(folio_uuid_pattern, id["identifierTypeId"]),
                    f"{json.dumps(id, indent=4)}- {m} - {json.dumps(record[0]['identifiers'], indent=4)}",
                )

        identifiers = [f["identifierTypeId"] for f in record[0]["identifiers"]]
        with self.subTest(id["value"]):
            self.assertTrue(
                all(identifiers), json.dumps(record[0]["identifiers"], indent=4)
            )

        with self.subTest(id["value"]):
            for i in identifiers:
                self.assertEqual(
                    1, len(str.split(i)), json.dumps(record[0]["identifiers"], indent=4)
                )

    def test_series(self):
        message = (
            "Should add series statements (800, 810, 811, 830, 440, 490) to series list"
        )
        xpath = "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']"
        record = self.default_map("test_series.xml", xpath)
        m = message + "\n" + record[1]
        # 800
        self.assertIn(
            "Joyce, James, 1882-1941. James Joyce archive", record[0]["series"], m
        )
        # 810
        self.assertIn(
            "United States. Dept. of the Army. Field manual", record[0]["series"], m
        )
        # 811
        self.assertIn(
            "International Congress of Nutrition (11th : 1978 : Rio de Janeiro, Brazil). Nutrition and food science ; v. 1",
            record[0]["series"],
            m,
        )
        # 830
        self.assertIn(
            "Philosophy of engineering and technology ; v. 21", record[0]["series"], m
        )
        self.assertIn(
            "American university studies. Foreign language instruction ; vol. 12",
            record[0]["series"],
            m,
        )
        # 440
        """self.assertIn(
            "Journal of polymer science. Part C, Polymer symposia ; no. 39",
            record[0]["series"],
            m,
        )"""
        # 490
        """self.assertIn(
            "Pediatric clinics of North America ; v. 2, no. 4", record[0]["series"], m
        )"""

    def test_series_deduped(self):
        message = "Should deduplicate identical series statements from 830 and 490 in series list"
        xpath = "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']"
        record = self.default_map("test_series_duplicates.xml", xpath)
        m = message + "\n" + record[1]
        # self.assertIn("Oracle Press book", record[0]["series"], m)
        self.assertIn("Oracle Press book", record[0]["series"], m)
        self.assertIn("McGraw-Hill technical education series", record[0]["series"], m)
        self.assertEqual(2, len(record[0]["series"]), m)

    def test_contributors(self):
        message = "Should add contributors (100, 111 700) to the contributors list"
        xpath = "//marc:datafield[@tag='100' or @tag='111' or @tag='700']"
        record = self.default_map("test_contributors.xml", xpath)
        contributors = list((c["name"] for c in record[0]["contributors"]))
        m = message + "\n" + record[1]
        with self.subTest("100, no contrib type indicated"):
            self.assertIn("Chin, Stephen, 1977-", contributors, m)
        with self.subTest("100$4"):
            self.assertIn("Presthus, Robert Vance", contributors, m)
        with self.subTest("100$ade4, unknown typeid, set type text to cartographer"):
            self.assertIn("Lous, Christian Carl, 1724-1804", contributors, m)
        with self.subTest("700$e (contributor)"):
            self.assertIn("Weaver, James L", contributors, m)
        # print(json.dumps(record[0]["contributors"], indent=4))
        with self.subTest("111$acde, no contrib type id"):
            self.assertIn("Wolfcon Durham 2018", contributors, m)
        with self.subTest("111$abbde4"):
            self.assertIn(
                "Kyōto Daigaku. Genshiro Jikkenjo. Senmon Kenkyūkai (2013 January 25)",
                contributors,
                m,
            )
        with self.subTest(
            "111$aee44  multiple relation types (author, illustrator), pick first one?"
        ):
            self.assertIn("Tupera Tupera (Firm)", contributors, m)

    def test_classifications(self):
        message = "Should add classifications (050, 082, 090, 086) to the classifications list"
        xpath = "//marc:datafield[@tag='050' or @tag='082' or @tag='090' or @tag='086']"
        record = self.default_map("test_classifications.xml", xpath)
        classes = list(c["classificationNumber"] for c in record[0]["classifications"])
        m = message + "\n" + record[1]
        with self.subTest("LoC 050"):
            self.assertIn("TK7895.E42 C45 2016", classes, m)
        with self.subTest("Dewey 082"):
            self.assertIn("004.165 C4412r 2015", classes, m)
        with self.subTest("LoC Local 090"):
            self.assertIn("HV6089 .M37 1989a", classes, m)
        with self.subTest("SuDOC 086"):
            self.assertIn("ITC 1.12:TA-503 (A)-18 AND 332-279", classes, m)

    def test_subjects(self):
        message = "Should add subjects (600, 610, 611, 630, 647, 648, 650, 651) to the subjects list"
        xpath = "//marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='647' or @tag='648' or @tag='650' or @tag='651']"
        record = self.default_map("test_subjects.xml", xpath)
        m = message + "\n" + record[1]
        with self.subTest("600$abcdq"):
            self.assertIn(
                "Kougeas, Sōkr. V. IV Diogenes, Emperor of the East, active 1068-1071. (Sōkratēs V.)",
                record[0]["subjects"],
                m,
            )
        with self.subTest("610$abcdn"):
            self.assertIn(
                "Frederick II, King of Prussia, 1712-1786. No. 2",
                record[0]["subjects"],
                m,
            )
        with self.subTest("611$acde"):
            self.assertIn(
                "Mississippi Valley Sanitary Fair (Venice, Italy). (1864 : ǂc Saint Louis, Mo.). Freedmen and Union Refugees' Department",
                record[0]["subjects"],
                m,
            )
        with self.subTest("630$adfhklst"):
            self.assertIn(
                "B.J. and the Bear. (1906) 1998. [medium] Manuscript. English New International [title]",
                record[0]["subjects"],
                m,
            )
        with self.subTest("648$avxyz"):
            self.assertIn(
                "Twentieth century Social life and customs", record[0]["subjects"], m
            )
        with self.subTest("650$abcdvxyz"):
            self.assertIn("Engineering Philosophy", record[0]["subjects"], m)
        with self.subTest("651$avxyz"):
            self.assertIn(
                "Aix-en-Provence (France) Philosophy. Early works to 1800",
                record[0]["subjects"],
                m,
            )

    def test_publication(self):
        message = "Should add publications (260$abc & 264$abc) to the publications list"
        xpath = "//marc:datafield[@tag='260' or @tag='264']"
        record = self.default_map("test_publications.xml", xpath)
        m = message + "\n" + record[1]
        with self.subTest("260$abc"):
            publication = {
                "publisher": "Elsevier",
                "place": "New York, N.Y",
                "dateOfPublication": "1984",
            }
            self.assertIn(publication, record[0]["publication"], m)
        with self.subTest("264$abc"):
            publication = {
                "publisher": "Springer",
                "place": "Cham",
                "dateOfPublication": "[2015]",
                "role": "Publication",
            }
            self.assertIn(publication, record[0]["publication"], m)

    def test_publication_frequency(self):
        message = "Should add publication frequency (310$ab & 321$ab) to the publicationFrequency list"
        xpath = "//marc:datafield[@tag='310' or @tag='321']"
        record = self.default_map("test_publication_frequency.xml", xpath)
        m = message + "\n" + record[1]
        self.assertEqual(2, len(record[0]["publicationFrequency"]), m)
        with self.subTest("310$ab"):
            self.assertIn("Varannan månad, 1983-", record[0]["publicationFrequency"], m)
        with self.subTest("321$ab"):
            self.assertIn(
                "Monthly, Mar. 1972-Dec. 1980", record[0]["publicationFrequency"], m
            )

    def test_publication_range(self):
        message = "Should add publication range (362$a) to the publicationRange list"
        xpath = "//marc:datafield[@tag='362']"
        record = self.default_map("test_publication_range.xml", xpath)
        m = message + "\n" + record[1]
        self.assertEqual(1, len(record[0]["publicationRange"]), m)
        self.assertIn("No 1-", record[0]["publicationRange"], m)

    def test_notes_50x(self):
        message = "Should add notes (500-510) to notes list"
        xpath = "//marc:datafield[@tag='500' or @tag='501' or @tag='502' or @tag='504' or @tag='505' or @tag='506' or @tag='508' or @tag='510']"
        record = self.default_map("test_notes_50x.xml", xpath)
        m = message + "\n" + record[1]
        notes = list([note["note"] for note in record[0]["notes"]])
        so = list([note["staffOnly"] for note in record[0]["notes"]])
        print(so)
        with self.subTest("staffOnly"):
            for s in so:
                self.assertEqual(type(s), bool)
        with self.subTest("500$a"):
            self.assertIn(
                '"Embedded application development for home and industry."--Cover',
                notes,
                m,
            )
        with self.subTest("500$3a5"):
            self.assertIn(
                "Cotsen copy: Published plain red wrappers with first and last leaves pasted to interior wrappers. NjP",
                notes,
                m,
            )
        with self.subTest("501$a5"):
            self.assertIn(
                "With: Humiliations follow'd with deliverances. Boston : Printed by B. Green; J. Allen for S. Philips, 1697. Bound together subsequent to publication. DLC",
                notes,
                m,
            )
        with self.subTest("502$bcd"):
            self.assertIn("M. Eng. University of Louisville 2013", notes, m)
        with self.subTest("504$ab"):
            self.assertIn("Includes bibliographical references. 19", notes, m)
        with self.subTest("506$a"):
            self.assertIn("Classified", notes, m)
        with self.subTest("507$b"):
            self.assertIn("Not drawn to scale", notes, m)
        with self.subTest("508$a"):
            self.assertIn(
                "Film editor, Martyn Down ; consultant, Robert F. Miller", notes, m
            )
        with self.subTest("508$a"):
            self.assertIn(
                "Film editor, Martyn Down ; consultant, Robert F. Miller", notes, m
            )
        with self.subTest("510$axb"):
            self.assertIn("Index medicus, 0019-3879, v1n1, 1984-", notes, m)

    def test_notes_51x(self):
        message = "Should add notes (511-518) to notes list"
        xpath = "//marc:datafield[@tag='511' or @tag='513' or @tag='514' or @tag='515' or @tag='516' or @tag='518']"
        record = self.default_map("test_notes_51x.xml", xpath)
        m = message + "\n" + record[1]
        notes = list([note["note"] for note in record[0]["notes"]])
        with self.subTest("511$a"):
            self.assertIn("Marshall Moss, violin ; Neil Roberts, harpsichord", notes, m)
        with self.subTest("513$ab"):
            self.assertIn(
                "Quarterly technical progress report; January-April 1, 1977", notes, m
            )
        with self.subTest("514$adef"):
            self.assertIn(
                "The map layer that displays Special Feature Symbols shows the approximate location of small (less than 2 acres in size) areas of soils... Quarter quadrangles edited and joined internally and to surrounding quads. All known errors corrected. The combination of spatial linework layer, Special Feature Symbols layer, and attribute data are considered a complete SSURGO dataset. The actual on ground transition between the area represented by the Special Feature Symbol and the surrounding soils generally is very narrow with a well defined edge. The center of the feature area was compiled and digitized as a point. The same standards for compilation and digitizing used for line data were applied to the development of the special feature symbols layer",
                notes,
                m,
            )
        with self.subTest("515$a"):
            self.assertIn(
                "Designation New series dropped with volume 38, 1908", notes, m
            )
        with self.subTest("516$a"):
            self.assertIn("Numeric (Summary statistics)", notes, m)
        with self.subTest("518$3dp"):
            self.assertIn(
                "3rd work 1981 November 25 Neues Gewandhaus, Leipzig", notes, m
            )

    def test_notes_52x(self):
        message = "Should add notes (520-525) to notes list"
        xpath = "//marc:datafield[@tag='520' or @tag='522' or @tag='524' or @tag='525']"
        record = self.default_map("test_notes_52x.xml", xpath)
        m = message + "\n" + record[1]
        notes = list([note["note"] for note in record[0]["notes"]])
        with self.subTest("520$a"):
            self.assertIn(
                '"Create embedded projects for personal and professional applications. Join the Internet of Things revolution with a project-based approach to building embedded Java applications. Written by recognized Java experts, this Oracle Press guide features a series of low-cost, DIY projects that gradually escalate your development skills. Learn how to set up and configure your Raspberry Pi, connect external hardware, work with the NetBeans IDE, and write and embed powerful Java applications. Raspberry Pi with Java: Programming the Internet of Things (IoT) covers hobbyist as well as professional home and industry applications."--Back cover',
                notes,
                m,
            )
        with self.subTest("522$a"):
            self.assertIn("County-level data from Virginia", notes, m)
        with self.subTest("524$a"):
            self.assertIn("Dakota usc", notes, m)
        with self.subTest("525$a"):
            self.assertIn("Supplements accompany some issues", notes, m)

    def test_notes_53x(self):
        message = "Should add notes (530-534) to notes list"
        xpath = "//marc:datafield[@tag='530' or @tag='532' or @tag='533' or @tag='534']"
        record = self.default_map("test_notes_53x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("530$a"):
            self.assertIn("Available on microfiche", notes, m)
        with self.subTest("532$a"):
            self.assertIn("Closed captioning in English", notes, m)
        with self.subTest("533$abcdfn5"):
            self.assertIn(
                "Electronic reproduction. Cambridge, Mass. Harvard College Library Digital Imaging Group, 2003 (Latin American pamphlet digital project at Harvard University ; 0005). Electronic reproduction from microfilm master negative produced by Harvard College Library Imaging Services. MH",
                notes,
                m,
            )
        with self.subTest("534$patn"):
            self.assertIn(
                "Originally issued: Frederick, John. Luck. Published in: Argosy, 1919",
                notes,
                m,
            )

    def test_notes_54x(self):
        message = "Should add notes (540-546) to notes list"
        xpath = "//marc:datafield[@tag='540' or @tag='541' or @tag='542' or @tag='544' or @tag='545' or @tag='546']"
        record = self.default_map("test_notes_54x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("540"):
            self.assertIn(
                "Recorded radio programs There are copyright and contractual restrictions applying to the reproduction of most of these recordings; Department of Treasury; Treasury contracts 7-A130 through 39-A179",
                notes,
                m,
            )
        with self.subTest("541"):
            self.assertIn(
                "5 diaries 25 cubic feet; Merriwether, Stuart; 458 Yonkers Road, Poughkeepsie, NY 12601; Purchase at auction; 19810924; 81-325; Jonathan P. Merriwether Estate; $7,850",
                notes,
                m,
            )
        with self.subTest("542"):
            self.assertIn(
                "Duchess Foods Government of Canada Copyright Services, Library and Archives Canada, Ottawa, Ont. Copyright 1963, par la Compagnie Canadienne de l'Exposition Universelle de 1967 1963 1963 Duchess Foods under copyright protection through Dec. 31, 2013 published ǂn Copyright not renewable. This work will enter the public domain on Jan. 1, 2014 Nov. 2010 Canada CaQMCCA Canada Copyright Services, Library and Archives Canada",
                notes,
                m,
            )
        with self.subTest("544"):
            self.assertIn(
                "Correspondence files; Burt Barnes papers; Also located at; State Historical Society of Wisconsin",
                notes,
                m,
            )
        with self.subTest("545"):
            self.assertIn(
                "The Faribault State School and Hospital provided care, treatment, training, and a variety of other services to mentally retarded individuals and their families. It was operated by the State of Minnesota from 1879 to 1998 under different administrative structures and with different names. A more detailed history of the Hospital may be found at http://www.mnhs.org/library/findaids/80881.html",
                notes,
                m,
            )
        with self.subTest("546"):
            self.assertIn("Marriage certificate German; Fraktur", notes, m)

    def test_notes_55x(self):
        message = "Should add notes (550-556) to notes list"
        xpath = "//marc:datafield[@tag='550' or @tag='552' or @tag='555' or @tag='556']"
        record = self.default_map("test_notes_55x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("550$a"):
            self.assertIn("Organ of the Potomac-side Naturalists' Club", notes, m)
        with self.subTest("552"):
            self.assertIn(
                "NYROADS The roads of New York, none NYROADS_TYPE The road types of New York, none 1 Interstate Highway, none 1-4 New York Road Types, none 1999010-19990201 unknown irregular",
                notes,
                m,
            )
        with self.subTest("555"):
            self.assertIn(
                "Finding aid Available in repository and on Internet; Folder level control; http://digital.library.pitt.edu/cgi-bin/f/findaid/findaid-idx?type=simple;c=ascead;view=text;subview=outline;didno=US-PPiU-ais196815",
                notes,
                m,
            )
        with self.subTest("556"):
            self.assertIn(
                "Disaster recovery : a model plan for libraries and information centers. 0959328971",
                notes,
                m,
            )

    def test_modes_of_issuance(self):
        message = "Should parse Mode of issuance correctly"
        xpath = "//marc:leader"
        with self.subTest("m"):
            record = self.default_map("test1.xml", xpath)
            moi = record[0]["modeOfIssuanceId"]
            m = message + "\n" + record[1]
            self.assertIn("9d18a02f-5897-4c31-9106-c9abb5c7ae8b", moi)

        with self.subTest("s"):
            record = self.default_map("test4.xml", xpath)
            moi = record[0]["modeOfIssuanceId"]
            m = message + "\n" + record[1]
            self.assertIn("068b5344-e2a6-40df-9186-1829e13cd344", moi)

    def test_notes_56x(self):
        message = "Should add notes (561-567) to notes list"
        xpath = "//marc:datafield[@tag='561' or @tag='562' or @tag='563' or @tag='565' or @tag='567']"
        record = self.default_map("test_notes_56x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("561$3a"):
            self.assertIn(
                "Family correspondence Originally collected by Henry Fitzhugh, willed to his wife Sarah Jackson Fitzhugh and given by her to her grandson Jonathan Irving Jackson, who collected some further information about his grandmother and the papers of their relatives and Cellarsville neighbors, the Arnold Fitzhugh's, before donating the materials along with his own papers as mayor of Cellarsville to the Historical Society",
                notes,
                m,
            )
        with self.subTest("562"):
            self.assertIn(
                "The best get better Sue Hershkowitz 2 copies; Originally given orally as a keynote address",
                notes,
                m,
            )
        with self.subTest("563"):
            self.assertIn(
                "Gold-tooled morocco binding by Benjamin West, approximately 1840. [URI] Uk",
                notes,
                m,
            )
        with self.subTest("565"):
            # TODO: can't be right, spreadsheet shoduld include subfield 3 i think
            self.assertIn(
                "Military petitioners files 11; name; address; date of birth; place of birth; date of application; dates of service; branch of service; date of induction; rank; latest occupation; dependents; pensioners; Civil War (1861-1865) veterans",
                notes,
                m,
            )
        with self.subTest("567"):
            self.assertIn("Continuous, deterministic, predictive", notes, m)

    def test_notes_58x(self):
        message = "Should add notes (580-586) to notes list"
        xpath = "//marc:datafield[@tag='580' or @tag='583' or @tag='586']"
        record = self.default_map("test_notes_58x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("580"):
            self.assertIn(
                "Forms part of the Frances Benjamin Johnston Collection", notes, m
            )
        with self.subTest("583"):
            self.assertIn(
                "scrapbooks (10 volumes) 1 cu. ft. microfilm 198303 at completion of arrangement 1983 master film schedule Thomas Swing",
                notes,
                m,
            )
        with self.subTest("586"):
            self.assertIn("Tempest fantasy Pulitzer prize in music, 2004", notes, m)

    def test_notes_59x(self):
        message = "Should add notes (590-599) to notes list"
        xpath = "//marc:datafield[@tag='590' or @tag='592' or @tag='599']"
        record = self.default_map("test_notes_59x.xml", xpath)
        notes = list([note["note"] for note in record[0]["notes"]])
        m = message + "\n" + record[1]
        with self.subTest("590$a"):
            self.assertIn("Labels reversed on library's copy", notes, m)

    def test_format(self):
        message = "Should parse Mode of issuance correctly"
        xpath = "//marc:datafield[@tag='337' or @tag='338']"
        with self.subTest("2-character code in 338"):
            record = self.default_map("test_carrier_and_format.xml", xpath)
            # print(json.dumps(record, sort_keys=True, indent=4))
            moi = record[0]["modeOfIssuanceId"]
            m = message + "\n" + record[1]
            self.assertEqual("9d18a02f-5897-4c31-9106-c9abb5c7ae8b", moi)

        with self.subTest("337+338"):
            record = self.default_map("test_carrier_and_format.xml", xpath)
            formats = record[0]["instanceFormatIds"]
            m = message + "\n" + record[1]
            self.assertIn("8d511d33-5e85-4c5d-9bce-6e3c9cd0c324", formats)

        with self.subTest("2 338$b"):
            record = self.default_map("test_carrier_and_format.xml", xpath)
            formats = record[0]["instanceFormatIds"]
            m = message + "\n" + record[1]
            self.assertEqual(4, len(formats))


if __name__ == "__main__":
    unittest.main()
