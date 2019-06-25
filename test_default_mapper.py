import unittest
import xml.etree.ElementTree as ET
from lxml import etree
import pymarc
import json
from collections import namedtuple
from jsonschema import validate
from marc_to_folio.default_mapper import DefaultMapper
from marc_to_folio.folio_client import FolioClient


class TestDefaultMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open('./tests/test_config.json') as settings_file:
            cls.config = json.load(settings_file,
                                   object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            cls.folio = FolioClient(cls.config)
            cls.mapper = DefaultMapper(cls.folio)
            cls.mapper = DefaultMapper(cls.folio)
            cls.instance_schema = cls.folio.get_instance_json_schema()

    def default_map(self, file_name, xpath):
        ns = {'marc': 'http://www.loc.gov/MARC21/slim',
              'oai': 'http://www.openarchives.org/OAI/2.0/'}
        file_path = r'./tests/test_data/{}'.format(file_name)
        record = pymarc.parse_xml_to_array(file_path)[0]
        result = self.mapper.parse_bib(record, "source")
        validate(result, self.instance_schema)
        root = etree.parse(file_path)
        data = str('')
        for element in root.xpath(xpath, namespaces=ns):
            data = ' '.join(
                [data, str(etree.tostring(element, pretty_print=True), 'utf-8')])
        return [result, data]

    def test_simple_title(self):
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map('test1.xml', xpath)
        self.assertEqual(
            'Modern Electrosynthetic Methods in Organic Chemistry', record[0]['title'])
        # TODO: test abcense of / for chalmers

    def test_composed_title(self):
        message = 'Should create a composed title (245) with the [a, b, k, n, p] subfields.'
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map('test_composed_title.xml', xpath)
        # self.assertFalse('/' in record['title'])
        self.assertEqual('The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment. / Steen Hyldgaard Christensen, Christelle Didier, Andrew Jamison, Martin Meganck, Carl Mitcham, Byron Newberry, editors.',
                         record[0]['title'], message + '\n' + record[1])

    def test_alternative_titles_246(self):
        message = 'Should match 246 to alternativeTitles'
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map('test3.xml', xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = 'Engineering identities, epistemologies and values'
        alt_titles = list((t['alternativeTitle'] for t
                           in record[0]['alternativeTitles']))
        self.assertIn(title, alt_titles, message + '\n' + record[1])

    def test_alternative_titles_130(self):
        message = 'Should match 130 to alternativeTitles'
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map('test4.xml', xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = "Les cahiers d'urbanisme"
        alt_titles = list((t['alternativeTitle'] for t
                           in record[0]['alternativeTitles']))
        self.assertIn(title, alt_titles, message + '\n' + record[1])

    def alternative_titles_246_and_130(self):
        message = 'Should match 246 to alternativeTitles when there is also 130'
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map('test4.xml', xpath)
        title = "Cahiers d'urbanisme et d'aménagement du territoire"
        alt_titles = list((t['alternativeTitle'] for t
                           in record[0]['alternativeTitles']))
        self.assertIn(title, alt_titles, message + '\n' + record[1])

    def alternative_titles_4(self):
        message = 'Should match 222 to alternativeTitles when there is also 130'
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map('test4.xml', xpath)
        # self.assertFalse(all('/' in t for t in record['alternativeTitles']))
        title = "Urbana tidskrifter"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        title = "Cahiers d'urbanisme et d'aménagement du territoire"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        title = "Les cahiers d'urbanisme"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])

    def test_editions(self):
        message = 'Should add editions (250) to the editions list and enforce unique'
        xpath = "//marc:datafield[@tag='250']"
        record = self.default_map('test_editions.xml', xpath)
        editions_stmts = ['8. uppl.', '[Revised]']
        for stmt in editions_stmts:
            self.assertIn(stmt, record[0]['editions'],
                          message + '\n' + record[1])

    def test_languages_041(self):
        message = 'Should add languages (041$a) to the languages list; ignores non-ISO languages'
        xpath = "//marc:datafield[@tag='041']"
        record = self.default_map('test_multiple_languages.xml', xpath)
        lang_codes = ['eng', 'ger', 'fre', 'ita']
        should_not_be_there = ['en_US', '###', 'zxx']
        for lang_code in should_not_be_there:
            self.assertNotIn(lang_code, record[0]['languages'],
                             message + '\n' + record[1])
        for lang_code in lang_codes:
            self.assertIn(lang_code, record[0]['languages'],
                          message + '\n' + record[1])

    def test_languages_008(self):
        message = 'Should add language found in 008 where there is no 041'
        xpath = "//marc:controlfield[@tag='008']"
        record = self.default_map('test_language_in_008.xml', xpath)
        self.assertIn('fre', record[0]['languages'],
                      message + '\n' + record[1])

    def test_physical_descriptions(self):
        message = 'Should add physical descriptions (300$abce)'
        xpath = "//marc:datafield[@tag='300']"
        record = self.default_map('test_physical_descriptions.xml', xpath)
        phy_des = 'xxxiv, 416 pages illustrations 24 cm.'
        self.assertIn(phy_des, record[0]['physicalDescriptions'],
                      message + '\n' + record[1])

    def test_index_title(self):
        message = 'Should trim title (245) by n-chars, as specified by indicator 2'
        xpath = "//marc:datafield[@tag='245']"
        record = self.default_map('test_index_title.xml', xpath)
        self.assertEqual("cahiers d'urbanisme", record[0]['indexTitle'],
                         message + '\n' + record[1])

    def test_alternative_titles_all(self):
        message = 'Should add all types of alternative titles: 130, 222, 240, 246, 247 '
        xpath = "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']"
        record = self.default_map('test_alternative_titles.xml', xpath)
        # 246
        title = "Engineering identities, epistemologies and values - remainder title"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        # 247
        title = "Medical world news annual review of medicine"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        # 240
        title = "Laws, etc. (Laws of Kenya : 1948)"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        # 222
        title = "Soviet astronomy letters"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])
        # 130
        title = "Star is born (Motion picture : 1954)"
        self.assertIn(title, (t['alternativeTitle'] for t
                              in record[0]['alternativeTitles']), message + '\n' + record[1])

    def test_identifiers(self):
        message = 'Should add identifiers: 010, 019, 020, 022, 024, 028, 035'
        xpath = "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']"
        record = self.default_map('test_identifiers.xml', xpath)
        m = message + '\n' + record[1]
        # TODO: Test identifier type id in additional mappers
        self.assertIn('2008011507', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('9780307264787', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('9780071842013', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0071842012', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('9780307264755', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('9780307264766', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('9780307264777', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0376-4583', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0027-3475', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0027-3476', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('1234-1232', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('1560-15605', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0046-2254', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('7822183031', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('M011234564', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('PJC 222013', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)898162644', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)898087359', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)930007675', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)942940565', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('0027-3473', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('62874189', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('244170452', (i['value'] for i in record[0]['identifiers']), m)
        self.assertIn('677051564', (i['value'] for i in record[0]['identifiers']), m)
 
    def test_series(self):
        message = 'Should add series statements (800, 810, 811, 830, 440, 490) to series list'
        xpath = "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']"
        record = self.default_map('test_series.xml', xpath)
        m = message + '\n' + record[1]
        # 800
        self.assertIn('Joyce, James, 1882-1941. James Joyce archive.', record[0]['series'], m)
        # 810
        self.assertIn('United States. Dept. of the Army. Field manual.', record[0]['series'], m)
        # 811
        self.assertIn('International Congress of Nutrition (11th : 1978 : Rio de Janeiro, Brazil). Nutrition and food science ; v. 1.', record[0]['series'], m)
        # 830
        self.assertIn('Philosophy of engineering and technology ; v. 21.', record[0]['series'], m)
        self.assertIn('American university studies. Foreign language instruction ; vol. 12.', record[0]['series'], m)
        # 440
        self.assertIn('Journal of polymer science. Part C, Polymer symposia ; no. 39', record[0]['series'], m)
        # 490
        self.assertIn('Pediatric clinics of North America ; v. 2, no. 4.', record[0]['series'], m)
'''

  let assert_16 = 'Should deduplicate identical series statements from 830 and 490 in series list'
  it(assert_16, function () {
    let data = fs.readFileSync('spec/dataconverter/test_series_duplicates.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].series).toContain('McGraw-Hill technical education series')
      if (logging) {
        log(data, assert_16,
          "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']",
          item[0], 'series')
      }
    })
  })

  // CONTRIBUTORS: 100, 111, 700
  let assert_17 = 'Should add contributors (100, 111 700) to the contributors list'
  it(assert_17, function () {
    let data = fs.readFileSync('spec/dataconverter/test_contributors.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 100, no contrib type indicated
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: '2b94c631-fca9-4892-a730-03ee529ffe2a',
          name: 'Chin, Stephen, 1977-',
          contributorTypeId: '',
          contributorTypeText: ''
        }
      )
      // 100$4
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: '2b94c631-fca9-4892-a730-03ee529ffe2a',
          name: 'Presthus, Robert Vance',
          contributorTypeId: '9c78babf-c596-4f6a-945f-652545f703aa',
          contributorTypeText: '' }
      )
      // 100$ade4, unknown typeid, set type text to cartographer
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: '2b94c631-fca9-4892-a730-03ee529ffe2a',
          name: 'Lous, Christian Carl, 1724-1804',
          contributorTypeId: '',
          contributorTypeText: 'cartographer' }
      )
      // 700$e (contributor)
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: '2b94c631-fca9-4892-a730-03ee529ffe2a',
          name: 'Weaver, James L.',
          contributorTypeId: '52942db3-9331-4d69-8c9d-f76a9085bad7',
          contributorTypeText: ''
        }
      )
      // 111$acde, no contrib type id
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: 'e8b311a6-3b21-03f2-2269-dd9310cb2d0a',
          name: 'Wolfcon Durham 2018',
          contributorTypeId: '',
          contributorTypeText: 'Hackathon'
        }
      )
      // 111$abbde4
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: 'e8b311a6-3b21-03f2-2269-dd9310cb2d0a',
          name: 'Kyōto Daigaku. Genshiro Jikkenjo. Senmon Kenkyūkai (2013 January 25)',
          contributorTypeId: '9c78babf-c596-4f6a-945f-652545f703aa',
          contributorTypeText: ''
        }
      )
      // 111$aee44
      // multiple relation types (author, illustrator), pick first one?
      expect(item[0].contributors).toContain(
        {
          contributorNameTypeId: 'e8b311a6-3b21-03f2-2269-dd9310cb2d0a',
          name: 'Tupera Tupera (Firm)',
          contributorTypeId: '9c78babf-c596-4f6a-945f-652545f703aa',
          contributorTypeText: ''
        }
      )
      if (logging) {
        log(data, assert_17,
          "//marc:datafield[@tag='100' or @tag='111' or @tag='700']",
          item[0], 'contributors')
      }
    })
  })

  // CLASSIFICATIONS: 050, 082, 090, 086
  let assert_18 = 'Should add classifications (050, 082, 090, 086) to the classifications list'
  it(assert_18, function () {
    let data = fs.readFileSync('spec/dataconverter/test_classifications.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // LoC 050
      expect(item[0].classifications).toContain({
        classificationNumber: 'TK7895.E42 C45 2016',
        classificationTypeId: 'ce176ace-a53e-4b4d-aa89-725ed7b2edac'
      })
      // Dewey 082
      expect(item[0].classifications).toContain({
        classificationNumber: '004.165',
        classificationTypeId: '42471af9-7d25-4f3a-bf78-60d29dcf463b'
      })
      // LoC local 090
      expect(item[0].classifications).toContain({
        classificationNumber: 'HV6089 .M37 1989a',
        classificationTypeId: 'ce176ace-a53e-4b4d-aa89-725ed7b2edac'
      })
      // SuDOC 086
      expect(item[0].classifications).toContain({
        classificationNumber: 'ITC 1.12:TA-503 (A)-18 AND 332-279',
        classificationTypeId: 'sudoc-identifier' })
      if (logging) {
        log(data, assert_18,
          "//marc:datafield[@tag='050' or @tag='082' or @tag='090' or @tag='086']",
          item[0], 'classifications')
      }
    })
  })

  // SUBJECTS: 600, 610, 611, 630, 647, 648, 650, 651
  let assert_19 = 'Should add subjects (600, 610, 611, 630, 647, 648, 650, 651) to the subjects list'
  it(assert_19, function () {
    let data = fs.readFileSync('spec/dataconverter/test_subjects.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 600$abcdq
      expect(item[0].subjects).toContain(
        'Kougeas, Sōkr. V. IV Diogenes, Emperor of the East, active 1068-1071. (Sōkratēs V.)')
      // 610$abcdn
      expect(item[0].subjects).toContain(
        'Frederick II, King of Prussia, 1712-1786. No. 2.')
      // 611$acde
      expect(item[0].subjects).toContain(
        'Mississippi Valley Sanitary Fair (Venice, Italy). (1864 : ǂc Saint Louis, Mo.). Freedmen and Union Refugees\' Department.')
      // 630$adfhklst
      expect(item[0].subjects).toContain(
        'B.J. and the Bear. (1906) 1998. [medium] Manuscript. English New International [title]')
      // 647$acdvxyz
      expect(item[0].subjects).toContain(
        'Bunker Hill, Battle of (Boston, Massachusetts : 1775)')
      // 648$avxyz
      expect(item[0].subjects).toContain(
        'Twentieth century Social life and customs.')
      // 650$abcdvxyz
      expect(item[0].subjects).toContain(
        'Engineering Philosophy.')
      // 651$avxyz
      expect(item[0].subjects).toContain(
        'Aix-en-Provence (France) Philosophy. Early works to 1800.')
      if (logging) {
        log(data, assert_19,
          "//marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='647' or @tag='648' or @tag='650' or @tag='651']",
          item[0], 'subjects')
      }
    })
  })

  // PUBLICATION: 260, 264
  let assert_20 = 'Should add publications (260$abc & 264$abc) to the publications list'
  it(assert_20, function () {
    let data = fs.readFileSync('spec/dataconverter/test_publications.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 260$abc
      expect(item[0].publication).toContain(
        {
          publisher: 'Elsevier',
          place: 'New York NY',
          dateOfPublication: '1984' }
      )
      // 264$abc
      expect(item[0].publication).toContain(
        {
          publisher: 'Springer',
          place: 'Cham',
          dateOfPublication: '[2015]',
          role: 'Publication' }
      )
      if (logging) {
        log(data, assert_20,
          "//marc:datafield[@tag='260' or @tag='264']",
          item[0], 'publication')
      }
    })
  })

  // PUBLICATION FREQUENCY: 310, 321
  let assert_21 = 'Should add publication frequency (310$ab & 321$ab) to the publicationFrequency list'
  it(assert_21, function () {
    let data = fs.readFileSync('spec/dataconverter/test_publication_frequency.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].publicationFrequency.length).toEqual(2) // check deduplication
      // 310$ab
      expect(item[0].publicationFrequency).toContain('Varannan månad, 1983-')
      // 321$ab
      expect(item[0].publicationFrequency).toContain('Monthly, Mar. 1972-Dec. 1980')
      if (logging) {
        log(data, assert_21,
          "//marc:datafield[@tag='310' or @tag='321']",
          item[0], 'publicationFrequency')
      }
    })
  })

  // PUBLICATION RANGE: 362
  let assert_22 = 'Should add publication range (362$a) to the publicationRange list'
  it(assert_22, function () {
    let data = fs.readFileSync('spec/dataconverter/test_publication_range.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].publicationRange.length).toEqual(1) // check deduplication
      expect(item[0].publicationRange).toContain('No 1-')
      if (logging) {
        log(data, assert_22,
          "//marc:datafield[@tag='362']",
          item[0], 'publicationRange')
      }
    })
  })

  // NOTES: 500-510
  let assert_23 = 'Should add notes (500-510) to notes list'
  it(assert_23, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_50x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 500$a
      expect(item[0].notes).toContain('"Embedded application development for home and industry."--Cover.')
      // 500$3a5
      expect(item[0].notes).toContain('Cotsen copy: Published plain red wrappers with first and last leaves pasted to interior wrappers. NjP')
      // 501$a5
      expect(item[0].notes).toContain('With: Humiliations follow\'d with deliverances. Boston : Printed by B. Green; J. Allen for S. Philips, 1697. Bound together subsequent to publication. DLC')
      // 502$bcd
      expect(item[0].notes).toContain('M. Eng. University of Louisville 2013')
      // 504$ab
      expect(item[0].notes).toContain('Includes bibliographical references. 19')
      // 506$a
      expect(item[0].notes).toContain('Classified.')
      // 507$b
      expect(item[0].notes).toContain('Not drawn to scale.')
      // 508$a
      expect(item[0].notes).toContain('Film editor, Martyn Down ; consultant, Robert F. Miller.')
      // 508$a
      expect(item[0].notes).toContain('Film editor, Martyn Down ; consultant, Robert F. Miller.')
      // 510$axb
      expect(item[0].notes).toContain('Index medicus, 0019-3879, v1n1, 1984-')
      if (logging) {
        log(data, assert_23,
          "//marc:datafield[@tag='500' or @tag='501' or @tag='502' or @tag='504' or @tag='505' or @tag='506' " +
          "or @tag='508' or @tag='510']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 511-518
  let assert_24 = 'Should add notes (511-518) to notes list'
  it(assert_24, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_51x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 511$a
      expect(item[0].notes).toContain('Marshall Moss, violin ; Neil Roberts, harpsichord.')
      // 513$ab
      expect(item[0].notes).toContain('Quarterly technical progress report; January-April 1, 1977.')
      // 514$adef
      expect(item[0].notes).toContain('The map layer that displays Special Feature Symbols shows the approximate location of small (less than 2 acres in size) areas of soils... Quarter quadrangles edited and joined internally and to surrounding quads. All known errors corrected. The combination of spatial linework layer, Special Feature Symbols layer, and attribute data are considered a complete SSURGO dataset.')
      // 515$a
      expect(item[0].notes).toContain('Designation New series dropped with volume 38, 1908.')
      // 516$a
      expect(item[0].notes).toContain('Numeric (Summary statistics).')
      // 518$3dp
      expect(item[0].notes).toContain('3rd work 1981 November 25 Neues Gewandhaus, Leipzig.')
      if (logging) {
        log(data, assert_24,
          "//marc:datafield[@tag='511' or @tag='513' or @tag='514' or @tag='515' or @tag='516' or @tag='518']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 520-525
  let assert_25 = 'Should add notes (520-525) to notes list'
  it(assert_25, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_52x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 520$a
      expect(item[0].notes).toContain('"Create embedded projects for personal and professional applications. Join the Internet of Things revolution with a project-based approach to building embedded Java applications. Written by recognized Java experts, this Oracle Press guide features a series of low-cost, DIY projects that gradually escalate your development skills. Learn how to set up and configure your Raspberry Pi, connect external hardware, work with the NetBeans IDE, and write and embed powerful Java applications. Raspberry Pi with Java: Programming the Internet of Things (IoT) covers hobbyist as well as professional home and industry applications."--Back cover.')
      // 522$a
      expect(item[0].notes).toContain('County-level data from Virginia.')
      // 524$a
      expect(item[0].notes).toContain('Dakota')
      // 525$a
      expect(item[0].notes).toContain('Supplements accompany some issues.')
      if (logging) {
        log(data, assert_25,
          "//marc:datafield[@tag='520' or @tag='522' or @tag='524' or @tag='525']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 530-534
  let assert_26 = 'Should add notes (530-534) to notes list'
  it(assert_26, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_53x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 530$a
      expect(item[0].notes).toContain('Available on microfiche.')
      // 532$a
      expect(item[0].notes).toContain('Closed captioning in English.')
      // 533$abcdfn5
      expect(item[0].notes).toContain('Electronic reproduction. Cambridge, Mass. Harvard College Library Digital Imaging Group, 2003 (Latin American pamphlet digital project at Harvard University ; 0005). Electronic reproduction from microfilm master negative produced by Harvard College Library Imaging Services. MH')
      // 534$patn
      expect(item[0].notes).toContain('Originally issued: Frederick, John. Luck. Published in: Argosy, 1919.')
      if (logging) {
        log(data, assert_26,
          "//marc:datafield[@tag='530' or @tag='532' or @tag='533' or @tag='534']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 540-546
  let assert_27 = 'Should add notes (540-546) to notes list'
  it(assert_27, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_54x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 540
      expect(item[0].notes).toContain('There are copyright and contractual restrictions applying to the reproduction of most of these recordings; Department of Treasury; Treasury contracts 7-A130 through 39-A179.')
      // 541
      expect(item[0].notes).toContain('5 diaries 25 cubic feet; Merriwether, Stuart; 458 Yonkers Road, Poughkeepsie, NY 12601; Purchase at auction; 19810924; 81-325; Jonathan P. Merriwether Estate; $7,850.')
      // 542
      expect(item[0].notes).toContain('Duchess Foods Government of Canada Copyright 1963, par la Compagnie Canadienne de l\'Exposition Universelle de 1967 1963 Nov. 2010 Copyright Services, Library and Archives Canada')
      // 544
      expect(item[0].notes).toContain('Burt Barnes papers; State Historical Society of Wisconsin.')
      // 545
      expect(item[0].notes).toContain('The Faribault State School and Hospital provided care, treatment, training, and a variety of other services to mentally retarded individuals and their families. It was operated by the State of Minnesota from 1879 to 1998 under different administrative structures and with different names. A more detailed history of the Hospital may be found at http://www.mnhs.org/library/findaids/80881.html')
      // 546
      expect(item[0].notes).toContain('Marriage certificate German; Fraktur.')
      if (logging) {
        log(data, assert_27,
          "//marc:datafield[@tag='540' or @tag='541' or @tag='542' or @tag='544' or @tag='545' or @tag='546']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 550-556
  let assert_28 = 'Should add notes (550-556) to notes list'
  it(assert_28, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_55x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 550$a
      expect(item[0].notes).toContain('Organ of the Potomac-side Naturalists\' Club.')
      // 552
      expect(item[0].notes).toContain('NYROADS The roads of New York, none unknown irregular.')
      // 555
      expect(item[0].notes).toContain('Available in repository and on Internet; Folder level control; http://digital.library.pitt.edu/cgi-bin/f/findaid/findaid-idx?type=simple;c=ascead;view=text;subview=outline;didno=US-PPiU-ais196815')
      // 556
      expect(item[0].notes).toContain('Disaster recovery : a model plan for libraries and information centers. 0959328971')
      if (logging) {
        log(data, assert_28,
          "//marc:datafield[@tag='550' or @tag='552' or @tag='555' or @tag='556']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 561-567
  let assert_29 = 'Should add notes (561-567) to notes list'
  it(assert_29, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_56x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 561$3a
      expect(item[0].notes).toContain('Family correspondence Originally collected by Henry Fitzhugh, willed to his wife Sarah Jackson Fitzhugh and given by her to her grandson Jonathan Irving Jackson, who collected some further information about his grandmother and the papers of their relatives and Cellarsville neighbors, the Arnold Fitzhugh\'s, before donating the materials along with his own papers as mayor of Cellarsville to the Historical Society.')
      // 562
      expect(item[0].notes).toContain('The best get better Sue Hershkowitz')
      // 563
      expect(item[0].notes).toContain('Gold-tooled morocco binding by Benjamin West, approximately 1840. Uk')
      // 565
      // todo: can't be right, spreadsheet shoduld include subfield 3 i think
      expect(item[0].notes).toContain('11;')
      // 567
      expect(item[0].notes).toContain('Continuous, deterministic, predictive.')
      if (logging) {
        log(data, assert_29,
          "//marc:datafield[@tag='561' or @tag='562' or @tag='563' or @tag='565' or @tag='567']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 580-586
  let assert_30 = 'Should add notes (580-586) to notes list'
  it(assert_30, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_58x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 580
      expect(item[0].notes).toContain('Forms part of the Frances Benjamin Johnston Collection.')
      // 583
      expect(item[0].notes).toContain('scrapbooks (10 volumes) 1 cu. ft. microfilm 198303 at completion of arrangement 1983 master film schedule Thomas Swing')
      // 586
      expect(item[0].notes).toContain('Pulitzer prize in music, 2004')
      if (logging) {
        log(data, assert_30,
          "//marc:datafield[@tag='580' or @tag='583' or @tag='586']",
          item[0], 'notes')
      }
    })
  })

  // NOTES: 590-599
  let assert_31 = 'Should add notes (590-599) to notes list'
  it(assert_31, function () {
    let data = fs.readFileSync('spec/dataconverter/test_notes_59x.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 590$a
      expect(item[0].notes).toContain('Labels reversed on library\'s copy.')
      // 592$a
      expect(item[0].notes).toContain('Copy in McGill Library\'s Osler Library of the History of Medicine, Robertson Collection copy 1: signature on title page, Jos. E. Dion, E.E.M., Montréal.')
      // 599$abcde
      expect(item[0].notes).toContain('c.2 2014 $25.00 pt art dept.')
      if (logging) {
        log(data, assert_31,
          "//marc:datafield[@tag='590' or @tag='592' or @tag='599']",
          item[0], 'notes')
      }
    })
  })

  // getRecord - Crashing example made into a test.
  it('Should just work. Get record example', function () {
    let data = fs.readFileSync('spec/dataconverter/test_get_record.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].title).toContain('Rysslands ekonomiska geografi')
    })
  })
})

function log (data, heading, xpath, json, key) {
  let XPath = require('xpath')
  let Dom = require('xmldom').DOMParser
  let select = XPath.useNamespaces({
    'marc': 'http://www.loc.gov/MARC21/slim',
    'oai': 'http://www.openarchives.org/OAI/2.0/' })
  const prettifyXml = require('prettify-xml')
  let doc = new Dom().parseFromString(data.toString())
  let nodes = select(xpath, doc)
  console.log(
    '\n' +
    '====================================================================================================\n' +
    heading + '\n' +
    '====================================================================================================\n'
  )
  nodes.forEach(function (xmlNode) {
    console.log(prettifyXml(xmlNode.toString()))
  })
  console.log('\n"' + key + '" : ' + JSON.stringify(json[key], null, '  ') + '\n')
}
'''

if __name__ == '__main__':
    unittest.main()
