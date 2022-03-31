import pytest
from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.library_configuration import (
    FolioRelease,
    HridHandling,
    LibraryConfiguration,
)
from migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
from lxml import etree
import pymarc
import json
import re
from folioclient.FolioClient import FolioClient
from migration_tools.migration_report import MigrationReport
from migration_tools.migration_tasks.bibs_transformer import BibsTransformer

xpath_245 = "//marc:datafield[@tag='245']"


@pytest.fixture(scope="module")
def mapper(pytestconfig) -> BibsRulesMapper:
    print("init")
    folio = FolioClient(
        pytestconfig.getoption("okapi_url"),
        pytestconfig.getoption("tenant_id"),
        pytestconfig.getoption("username"),
        pytestconfig.getoption("password"),
    )
    conf = BibsTransformer.TaskConfiguration(
        name="test",
        migration_task_type="BibsTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour="sierra",
    )
    lib = LibraryConfiguration(
        okapi_url=pytestconfig.getoption("okapi_url"),
        tenant_id=pytestconfig.getoption("tenant_id"),
        okapi_username=pytestconfig.getoption("username"),
        okapi_password=pytestconfig.getoption("password"),
        folio_release=FolioRelease.kiwi,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    return BibsRulesMapper(folio, lib, conf)


def default_map(file_name, xpath, the_mapper):
    ns = {
        "marc": "https://www.loc.gov/MARC21/slim",
        "oai": "https://www.openarchives.org/OAI/2.0/",
    }
    file_path = f"./tests/test_data/default/{file_name}"
    record = pymarc.parse_xml_to_array(file_path)[0]
    result = the_mapper.parse_bib(["legacy_id"], record, False)
    root = etree.parse(file_path)
    data = str("")
    for element in root.xpath(xpath, namespaces=ns):
        data = " ".join(
            [data, str(etree.tostring(element, pretty_print=True), "utf-8")]
        )
    # print(json.dumps(rec, indent=4, sort_keys=True))
    return [result, data]


def test_simple_title(mapper):
    record = default_map("test1.xml", xpath_245, mapper)
    instance_identifiers = set(["a", "b"])
    assert "Modern Electrosynthetic Methods in Organic Chemistry" == record[0]["title"]
    with pytest.raises(TransformationRecordFailedError):
        BibsProcessor.get_valid_instance_ids(
            record[0], ["a", "b"], instance_identifiers, MigrationReport()
        )


def test_simple_title2(mapper):
    record = default_map("test1.xml", xpath_245, mapper)
    instance_identifiers = set(["c", "d"])
    ids = BibsProcessor.get_valid_instance_ids(
        record[0], ["a", "b"], instance_identifiers, MigrationReport()
    )
    assert "a" in ids
    assert "b" in ids


def test_simple_title3(mapper):
    record = default_map("test1.xml", xpath_245, mapper)
    instance_identifiers = set(["b", "c", "d"])
    ids = BibsProcessor.get_valid_instance_ids(
        record[0], ["a", "b"], instance_identifiers, MigrationReport()
    )
    assert ids == ["a"]


def test_multiple336s(mapper):
    xpath = "//marc:datafield[@tag='336']"
    record = default_map("test_multiple_336.xml", xpath, mapper)
    assert "bd068f05-9de5-40c4-8f5e-e164a625b26f" in record[0]["instanceTypeId"]


def test_strange_isbn(mapper):
    xpath = "//marc:datafield[@tag='020']"
    record = default_map("isbn_c.xml", xpath, mapper)
    assert record[0].get("identifiers", None)
    identifiers = list(f["identifierTypeId"] for f in record[0]["identifiers"])
    assert all(identifiers)
    for i in identifiers:
        assert 1 == len(str.split(i))


def test_composed_title(mapper):
    message = "Should create a composed title (245) with the [a, b, k, n, p] subfields"
    record = default_map("test_composed_title.xml", xpath_245, mapper)
    # self.assertFalse('/' in record['title'])
    assert (
        "The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment. / Steen Hyldgaard Christensen, Christelle Didier, Andrew Jamison, Martin Meganck, Carl Mitcham, Byron Newberry, editors."
        == record[0]["title"]
    )


def test_alternative_titles_246(mapper):
    message = "Should match 246 to alternativeTitles"
    xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
    record = default_map("test3.xml", xpath, mapper)
    # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
    title = "Engineering identities, epistemologies and values"
    alt_titles = list((t["alternativeTitle"] for t in record[0]["alternativeTitles"]))
    assert title in alt_titles  # message


def test_alternative_titles_130(mapper):
    message = "Should match 130 to alternativeTitles"
    xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
    record = default_map("test4.xml", xpath, mapper)
    # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
    title = "Les cahiers d'urbanisme"
    alt_titles = list((t["alternativeTitle"] for t in record[0]["alternativeTitles"]))
    assert title in alt_titles  #


def alternative_titles_246_and_130(mapper):
    message = "Should match 246 to alternativeTitles when there is also 130"
    xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
    record = default_map("test4.xml", xpath, mapper)
    title = "Cahiers d'urbanisme et d'aménagement du territoire"
    alt_titles = list((t["alternativeTitle"] for t in record[0]["alternativeTitles"]))
    assert title in alt_titles


def alternative_titles_4(mapper):
    message = "Should match 222 to alternativeTitles when there is also 130"
    xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
    record = default_map("test4.xml", xpath, mapper)
    # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
    title = "Urbana tidskrifter"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])
    # message + "\n" + record[1],

    title = "Cahiers d'urbanisme et d'aménagement du territoire 57/58/59"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])
    # message + "\n" + record[1],
    title = "Les cahiers d'urbanisme"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])


def test_editions(mapper):
    message = "Should add editions (250) to the editions list and enforce unique"
    xpath = "//marc:datafield[@tag='250']"
    record = default_map("test_editions.xml", xpath, mapper)
    editions_stmts = ["8. uppl", "[Revised]"]
    for stmt in editions_stmts:
        assert stmt in record[0]["editions"]


def test_languages_041(mapper):
    message = (
        "Should add languages (041$a) to the languages list; ignores non-ISO languages"
    )
    xpath = "//marc:datafield[@tag='041']"
    record = default_map("test_multiple_languages.xml", xpath, mapper)
    lang_codes = ["eng", "ger", "fre", "ita"]
    should_not_be_there = ["en_US", "###", "zxx"]
    for lang_code in should_not_be_there:
        assert lang_code not in record[0]["languages"]
    for lang_code in lang_codes:
        assert lang_code in record[0]["languages"]


def test_languages_008(mapper):
    message = "Should add language found in 008 where there is no 041"
    xpath = "//marc:controlfield[@tag='008']"
    record = default_map("test_language_in_008.xml", xpath, mapper)
    assert "fre", record[0]["languages"]


def test_physical_descriptions(mapper):
    message = "Should add physical descriptions (300$abce)"
    xpath = "//marc:datafield[@tag='300']"
    record = default_map("test_physical_descriptions.xml", xpath, mapper)
    phy_des = "xxxiv, 416 pages illustrations 24 cm."
    assert phy_des in record[0]["physicalDescriptions"]


def test_index_title(mapper):
    record = default_map("test_index_title.xml", xpath_245, mapper)
    assert "Cahiers d'urbanisme" == record[0]["indexTitle"]


def test_alternative_titles_all(mapper):
    message = "Should add all types of alternative titles: 130, 222, 240, 246, 247 "
    xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
    record = default_map("test_alternative_titles.xml", xpath, mapper)
    # 246
    title = "Engineering identities, epistemologies and values remainder title"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])
    # 247
    title = "Medical world news annual review of medicine"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])
    # 240
    title = "Laws, etc. (Laws of Kenya : 1948)"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])
    # 130
    title = "Star is born (Motion picture : 1954)"
    assert title in list(t["alternativeTitle"] for t in record[0]["alternativeTitles"])


def test_identifiers(mapper):
    message = "Should add identifiers: 010, 019, 020, 022, 024, 028, 035"
    xpath = "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']"
    record = default_map("test_identifiers.xml", xpath, mapper)

    expected_concatenated_identifiers = [
        ["9780307264755", "9780307264766", "9780307264777"],
        ["0027-3473", "1560-15605", "0046-2254"],
        ["a 1", "a 2"],
    ]
    expected_identifiers = [
        "(OCoLC)ocn898162644",
        "19049386",
        "PJC 222013 Paris Jazz Corner Productions",
        "1234-1231",
        # "62874189", Add back with Kiwi
        "2008011507",
        "9780307264787",
        "9780071842013 (paperback) 200 SEK",
        "0071842012 (paperback)",
        "0376-4583",
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
    ids_in_rec = list([id["value"] for id in record[0]["identifiers"]])

    for id in expected_identifiers:
        assert id in ids_in_rec

    folio_uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"

    type_ids_in_recs = list([id["identifierTypeId"] for id in record[0]["identifiers"]])
    assert all(re.match(folio_uuid_pattern, type_id) for type_id in type_ids_in_recs)
    identifiers = [f["value"] for f in record[0]["identifiers"]]
    assert all(identifiers), json.dumps(identifiers, indent=4)

    for i in type_ids_in_recs:
        assert len(str.split(i)) == 1


def test_series(mapper):
    message = (
        "Should add series statements (800, 810, 811, 830, 440, 490) to series list"
    )
    xpath = "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']"
    record = default_map("test_series.xml", xpath, mapper)
    m = message + "\n" + record[1]
    # 800
    assert "Joyce, James, 1882-1941. James Joyce archive" in record[0]["series"]
    # 810
    assert "United States. Dept. of the Army. Field manual", record[0]["series"]
    # 811
    assert (
        "International Congress of Nutrition (11th : 1978 : Rio de Janeiro, Brazil). Nutrition and food science ; v. 1"
        in record[0]["series"]
    )  # 830
    assert "Philosophy of engineering and technology ; v. 21" in record[0]["series"]
    assert (
        "American university studies. Foreign language instruction ; vol. 12"
        in record[0]["series"]
    )  # 440

    # 490
    """assert "Pediatric clinics of North America ; v. 2, no. 4", record[0]["series"]   )"""


def test_contributors(mapper):
    message = "Should add contributors (100, 111 700) to the contributors list"
    xpath = "//marc:datafield[@tag='100' or @tag='111' or @tag='700']"
    record = default_map("test_contributors.xml", xpath, mapper)
    contributors = list((c["name"] for c in record[0]["contributors"]))
    m = message + "\n" + record[1]
    assert "Chin, Stephen, 1977-" in contributors
    assert "Presthus, Robert Vance" in contributors
    assert "Lous, Christian Carl, 1724-1804" in contributors
    assert "Weaver, James L" in contributors
    assert "Wolfcon Durham 2018" in contributors
    assert (
        "Kyōto Daigaku. Genshiro Jikkenjo. Senmon Kenkyūkai (2013 January 25)"
        in contributors
    )
    assert "Tupera Tupera (Firm)" in contributors


def test_classifications(mapper):
    message = (
        "Should add classifications (050, 082, 090, 086) to the classifications list"
    )
    xpath = "//marc:datafield[@tag='050' or @tag='082' or @tag='090' or @tag='086']"
    record = default_map("test_classifications.xml", xpath, mapper)
    classes = list(c["classificationNumber"] for c in record[0]["classifications"])
    m = message + "\n" + record[1]
    # "LoC 050"):
    assert "TK7895.E42 C45 2016", classes
    # "Dewey 082"):
    assert "004.165 C4412r 2015", classes
    # "LoC Local 090"):
    assert "HV6089 .M37 1989a", classes
    # "SuDOC 086"):
    assert "ITC 1.12:TA-503 (A)-18 AND 332-279", classes


def test_subjects(mapper):
    message = "Should add subjects (600, 610, 611, 630, 647, 648, 650, 651) to the subjects list"
    xpath = "//marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='647' or @tag='648' or @tag='650' or @tag='651']"
    record = default_map("test_subjects.xml", xpath, mapper)
    m = message + "\n" + record[1]
    assert (
        "Kougeas, Sōkr. V. IV Diogenes, Emperor of the East, active 1068-1071. (Sōkratēs V.)"
        in record[0]["subjects"]
    )  # with self.subTest("610$abcdn"):
    assert "Frederick II, King of Prussia, 1712-1786. No. 2" in record[0]["subjects"]
    assert (
        "Mississippi Valley Sanitary Fair (Venice, Italy). (1864 : ǂc Saint Louis, Mo.). Freedmen and Union Refugees' Department"
        in record[0]["subjects"]
    )
    assert (
        "B.J. and the Bear. (1906) 1998. [medium] Manuscript. English New International [title]"
        in record[0]["subjects"]
    )
    assert "Twentieth century Social life and customs" in record[0]["subjects"]
    assert "Engineering Philosophy", record[0]["subjects"]
    assert (
        "Aix-en-Provence (France) Philosophy. Early works to 1800"
        in record[0]["subjects"]
    )


def test_publication(mapper):
    message = "Should add publications (260$abc & 264$abc) to the publications list"
    xpath = "//marc:datafield[@tag='260' or @tag='264']"
    record = default_map("test_publications.xml", xpath, mapper)
    m = message + "\n" + record[1]
    publication = {
        "publisher": "Elsevier",
        "place": "New York, N.Y",
        "dateOfPublication": "1984",
    }
    assert publication in record[0]["publication"]
    # with self.subTest("264$abc"):
    publication = {
        "publisher": "Springer",
        "place": "Cham",
        "dateOfPublication": "[2015]",
        "role": "Publication",
    }
    assert publication in record[0]["publication"]


def test_publication_frequency(mapper):
    message = "Should add publication frequency (310$ab & 321$ab) to the publicationFrequency list"
    xpath = "//marc:datafield[@tag='310' or @tag='321']"
    record = default_map("test_publication_frequency.xml", xpath, mapper)
    m = message + "\n" + record[1]
    assert 2 == len(record[0]["publicationFrequency"])
    # with self.subTest("310$ab"):
    assert "Varannan månad, 1983-" in record[0]["publicationFrequency"]
    # with self.subTest("321$ab"):
    assert "Monthly, Mar. 1972-Dec. 1980" in record[0]["publicationFrequency"]


def test_publication_range(mapper):
    message = "Should add publication range (362$a) to the publicationRange list"
    xpath = "//marc:datafield[@tag='362']"
    record = default_map("test_publication_range.xml", xpath, mapper)
    m = message + "\n" + record[1]
    assert 1 == len(record[0]["publicationRange"])
    assert "No 1-" in record[0]["publicationRange"]


def test_notes_50x(mapper):
    message = "Should add notes (500-510) to notes list"
    xpath = "//marc:datafield[@tag='500' or @tag='501' or @tag='502' or @tag='504' or @tag='505' or @tag='506' or @tag='508' or @tag='510']"
    record = default_map("test_notes_50x.xml", xpath, mapper)
    m = message + "\n" + record[1]
    notes = list([note["note"] for note in record[0]["notes"]])
    so = list([note["staffOnly"] for note in record[0]["notes"]])
    print(so)
    # with self.subTest("staffOnly"):
    for s in so:
        assert type(s) is bool
    # with self.subTest("500$a"):
    assert '"Embedded application development for home and industry."--Cover' in notes
    # with self.subTest("500$3a5"):
    assert (
        "Cotsen copy: Published plain red wrappers with first and last leaves pasted to interior wrappers. NjP"
        in notes
    )
    # with self.subTest("501$a5"):
    assert (
        "With: Humiliations follow'd with deliverances. Boston : Printed by B. Green; J. Allen for S. Philips, 1697. Bound together subsequent to publication. DLC"
        in notes
    )
    # with self.subTest("502$bcd"):
    assert "M. Eng. University of Louisville 2013" in notes
    # with self.subTest("504$ab"):
    assert "Includes bibliographical references. 19" in notes
    # with self.subTest("506$a"):
    assert "Classified" in notes
    # with self.subTest("507$b"):
    assert "Not drawn to scale" in notes
    # with self.subTest("508$a"):
    assert "Film editor, Martyn Down ; consultant, Robert F. Miller" in notes
    # with self.subTest("508$a"):
    assert "Film editor, Martyn Down ; consultant, Robert F. Miller" in notes
    # with self.subTest("510$axb"):
    assert "Index medicus, 0019-3879, v1n1, 1984-" in notes


def test_notes_51x(mapper):
    message = "Should add notes (511-518) to notes list"
    xpath = "//marc:datafield[@tag='511' or @tag='513' or @tag='514' or @tag='515' or @tag='516' or @tag='518']"
    record = default_map("test_notes_51x.xml", xpath, mapper)
    m = message + "\n" + record[1]
    notes = list([note["note"] for note in record[0]["notes"]])
    # "511$a"):
    assert "Marshall Moss, violin ; Neil Roberts, harpsichord" in notes
    # "513$ab"):
    assert "Quarterly technical progress report; January-April 1, 1977" in notes
    # "514$adef"):
    assert (
        "The map layer that displays Special Feature Symbols shows the approximate location of small (less than 2 acres in size) areas of soils... Quarter quadrangles edited and joined internally and to surrounding quads. All known errors corrected. The combination of spatial linework layer, Special Feature Symbols layer, and attribute data are considered a complete SSURGO dataset. The actual on ground transition between the area represented by the Special Feature Symbol and the surrounding soils generally is very narrow with a well defined edge. The center of the feature area was compiled and digitized as a point. The same standards for compilation and digitizing used for line data were applied to the development of the special feature symbols layer"
        in notes
    )
    # "515$a"):
    assert "Designation New series dropped with volume 38, 1908" in notes
    # "516$a"):
    assert "Numeric (Summary statistics)" in notes
    # "518$3dp"):
    assert "3rd work 1981 November 25 Neues Gewandhaus, Leipzig" in notes


def test_notes_52x(mapper):
    message = "Should add notes (520-525) to notes list"
    xpath = "//marc:datafield[@tag='520' or @tag='522' or @tag='524' or @tag='525']"
    record = default_map("test_notes_52x.xml", xpath, mapper)
    m = message + "\n" + record[1]
    notes = list([note["note"] for note in record[0]["notes"]])
    # "520$a"):
    assert (
        '"Create embedded projects for personal and professional applications. Join the Internet of Things revolution with a project-based approach to building embedded Java applications. Written by recognized Java experts, this Oracle Press guide features a series of low-cost, DIY projects that gradually escalate your development skills. Learn how to set up and configure your Raspberry Pi, connect external hardware, work with the NetBeans IDE, and write and embed powerful Java applications. Raspberry Pi with Java: Programming the Internet of Things (IoT) covers hobbyist as well as professional home and industry applications."--Back cover'
        in notes
    )  # "522$a"):
    assert "County-level data from Virginia" in notes
    # "524$a"):
    assert "Dakota usc" in notes
    # "525$a"):
    assert "Supplements accompany some issues" in notes


def test_notes_53x(mapper):
    message = "Should add notes (530-534) to notes list"
    xpath = "//marc:datafield[@tag='530' or @tag='532' or @tag='533' or @tag='534']"
    record = default_map("test_notes_53x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "530$a"):
    assert "Available on microfiche" in notes
    # "532$a"):
    assert "Closed captioning in English" in notes
    # "533$abcdfn5"):
    assert (
        "Electronic reproduction. Cambridge, Mass. Harvard College Library Digital Imaging Group, 2003 (Latin American pamphlet digital project at Harvard University ; 0005). Electronic reproduction from microfilm master negative produced by Harvard College Library Imaging Services. MH"
        in notes
    )  # "534$patn"):
    assert (
        "Originally issued: Frederick, John. Luck. Published in: Argosy, 1919" in notes
    )


def test_notes_54x(mapper):
    message = "Should add notes (540-546) to notes list"
    xpath = "//marc:datafield[@tag='540' or @tag='541' or @tag='542' or @tag='544' or @tag='545' or @tag='546']"
    record = default_map("test_notes_54x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "540"):
    assert (
        "Recorded radio programs There are copyright and contractual restrictions applying to the reproduction of most of these recordings; Department of Treasury; Treasury contracts 7-A130 through 39-A179"
        in notes
    )
    # "541"):
    assert (
        "5 diaries 25 cubic feet; Merriwether, Stuart; 458 Yonkers Road, Poughkeepsie, NY 12601; Purchase at auction; 19810924; 81-325; Jonathan P. Merriwether Estate; $7,850"
        in notes
    )
    # "542"):
    assert (
        "Duchess Foods Government of Canada Copyright Services, Library and Archives Canada, Ottawa, Ont. Copyright 1963, par la Compagnie Canadienne de l'Exposition Universelle de 1967 1963 1963 Duchess Foods under copyright protection through Dec. 31, 2013 published ǂn Copyright not renewable. This work will enter the public domain on Jan. 1, 2014 Nov. 2010 Canada CaQMCCA Canada Copyright Services, Library and Archives Canada"
        in notes
    )
    # "544"):
    assert (
        "Correspondence files; Burt Barnes papers; Also located at; State Historical Society of Wisconsin"
        in notes
    )
    # "545"):
    assert (
        "The Faribault State School and Hospital provided care, treatment, training, and a variety of other services to mentally retarded individuals and their families. It was operated by the State of Minnesota from 1879 to 1998 under different administrative structures and with different names. A more detailed history of the Hospital may be found at http://www.mnhs.org/library/findaids/80881.html"
        in notes
    )
    # "546"):
    assert "Marriage certificate German; Fraktur" in notes


def test_notes_55x(mapper):
    message = "Should add notes (550-556) to notes list"
    xpath = "//marc:datafield[@tag='550' or @tag='552' or @tag='555' or @tag='556']"
    record = default_map("test_notes_55x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "550$a"):
    assert "Organ of the Potomac-side Naturalists' Club" in notes
    # "552"):
    assert (
        "NYROADS The roads of New York, none NYROADS_TYPE The road types of New York, none 1 Interstate Highway, none 1-4 New York Road Types, none 1999010-19990201 unknown irregular"
        in notes
    )  # "555"):
    assert (
        "Finding aid Available in repository and on Internet; Folder level control; http://digital.library.pitt.edu/cgi-bin/f/findaid/findaid-idx?type=simple;c=ascead;view=text;subview=outline;didno=US-PPiU-ais196815"
        in notes
    )  # "556"):
    assert (
        "Disaster recovery : a model plan for libraries and information centers. 0959328971"
        in notes
    )


def test_modes_of_issuance(mapper):
    message = "Should parse Mode of issuance correctly"
    xpath = "//marc:leader"
    # "m"):
    record = default_map("test1.xml", xpath, mapper)
    moi = record[0]["modeOfIssuanceId"]
    m = message + "\n" + record[1]
    assert "9d18a02f-5897-4c31-9106-c9abb5c7ae8b" == moi

    # "s"):
    record = default_map("test4.xml", xpath, mapper)
    moi = record[0]["modeOfIssuanceId"]
    m = message + "\n" + record[1]
    assert "363895f3-3c52-421e-8f78-9ec105ec15f1" == moi


def test_notes_56x(mapper):
    message = "Should add notes (561-567) to notes list"
    xpath = "//marc:datafield[@tag='561' or @tag='562' or @tag='563' or @tag='565' or @tag='567']"
    record = default_map("test_notes_56x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "561$3a"):
    assert (
        "Family correspondence Originally collected by Henry Fitzhugh, willed to his wife Sarah Jackson Fitzhugh and given by her to her grandson Jonathan Irving Jackson, who collected some further information about his grandmother and the papers of their relatives and Cellarsville neighbors, the Arnold Fitzhugh's, before donating the materials along with his own papers as mayor of Cellarsville to the Historical Society"
        in notes
    )
    # "562"):
    assert (
        "The best get better Sue Hershkowitz 2 copies; Originally given orally as a keynote address"
        in notes
    )
    # "563"):
    assert (
        "Gold-tooled morocco binding by Benjamin West, approximately 1840. [URI] Uk"
        in notes
    )
    # "565"):
    # TODO: can't be right, spreadsheet shoduld include subfield 3 i think
    assert (
        "Military petitioners files 11; name; address; date of birth; place of birth; date of application; dates of service; branch of service; date of induction; rank; latest occupation; dependents; pensioners; Civil War (1861-1865) veterans"
        in notes
    )
    # "567"):
    assert "Continuous, deterministic, predictive" in notes


def test_notes_58x(mapper):
    message = "Should add notes (580-586) to notes list"
    xpath = "//marc:datafield[@tag='580' or @tag='583' or @tag='586']"
    record = default_map("test_notes_58x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "580"):
    assert "Forms part of the Frances Benjamin Johnston Collection" in notes
    # "583"):
    assert (
        "scrapbooks (10 volumes) 1 cu. ft. microfilm 198303 at completion of arrangement 1983 master film schedule Thomas Swing"
        in notes
    )
    # "586"):
    assert "Tempest fantasy Pulitzer prize in music, 2004" in notes


def test_notes_59x(mapper):
    message = "Should add notes (590-599) to notes list"
    xpath = "//marc:datafield[@tag='590' or @tag='592' or @tag='599']"
    record = default_map("test_notes_59x.xml", xpath, mapper)
    notes = list([note["note"] for note in record[0]["notes"]])
    m = message + "\n" + record[1]
    # "590$a"):
    assert "Labels reversed on library's copy" in notes


def test_format(mapper):
    message = "Should parse Mode of issuance correctly"
    xpath = "//marc:datafield[@tag='337' or @tag='338']"
    # "2-character code in 338"):
    record = default_map("test_carrier_and_format.xml", xpath, mapper)
    # print(json.dumps(record, sort_keys=True, indent=4))
    moi = record[0]["modeOfIssuanceId"]
    m = message + "\n" + record[1]
    assert "9d18a02f-5897-4c31-9106-c9abb5c7ae8b" == moi

    # "337+338"):
    record = default_map("test_carrier_and_format.xml", xpath, mapper)
    formats = record[0]["instanceFormatIds"]
    m = message + "\n" + record[1]
    assert "b1d001a3-57ae-4e7c-aa6e-066d764a5fe7" in formats

    # "2 338$b"):
    record = default_map("test_carrier_and_format.xml", xpath, mapper)
    formats = record[0]["instanceFormatIds"]
    m = message + "\n" + record[1]
    assert 4 == len(formats)
