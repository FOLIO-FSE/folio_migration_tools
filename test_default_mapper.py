import unittest
import pymarc
import json
from collections import namedtuple
from jsonschema import validate
from marc_to_folio.default_mapper import DefaultMapper
from marc_to_folio.folio_client import FolioClient


class TestDefaultMapper(unittest.TestCase):
    def setUp(self):
        with open('./tests/test_config.json') as settings_file:
            self.config = json.load(settings_file,
                                    object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            self.folio = FolioClient(self.config)
            self.mapper = DefaultMapper(self.folio)
            self.instance_schema = self.folio.get_instance_json_schema()

    def test_simple_title(self):
        file_path = './tests/test_data/test1.xml'
        record = pymarc.parse_xml_to_array(file_path)[0]
        mapper = DefaultMapper(self.folio)
        result = mapper.parse_bib(record, "source")
        validate(result, self.instance_schema)
        self.assertEqual('Modern Electrosynthetic Methods in Organic Chemistry', result['title'])
        # TODO: test abcense of / for chalmers





'''// SIMPLE TITLE (245)
  let assert_1 = 'Should match title (245)'
  it(assert_1, function () {
    let data = fs.readFileSync('spec/dataconverter/test1.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].title).not.toContain('/')
      expect(item[0].title).toBe('Modern Electrosynthetic Methods in Organic Chemistry')
      if (logging) {
        log(data, assert_1, "//marc:datafield[@tag='245']", item[0], 'title')
      }
    })
  })
  // COMPOSED TITLE
  let assert_2 = 'Should create a composed title (245) with the [a, b, k, n, p] subfields.'
  it(assert_2, function () {
    let data = fs.readFileSync('spec/dataconverter/test_composed_title.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].title).not.toContain('/')
      expect(item[0].title).toBe(
        'The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment.'
      )
      if (logging) {
        log(data, assert_2, "//marc:datafield[@tag='245']", item[0], 'title')
      }
    })
  })

  // ALTERNATIVE TITLES (246)
  let assert_3 = 'Should match 246 to alternativeTitles'
  it(assert_3, function () {
    let data = fs.readFileSync('spec/dataconverter/test3.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].alternativeTitles).not.toContain('/')
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Engineering identities, epistemologies and values' }
      )
      if (logging) {
        log(data, assert_3,
          "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']",
          item[0], 'alternativeTitles')
      }
    })
  })

  let assert_4 = 'Should match 130 to alternativeTitles'
  it(assert_4, function () {
    let data = fs.readFileSync('spec/dataconverter/test4.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].alternativeTitles).toContain( { alternativeTitle: "Les cahiers d'urbanisme" })
      if (logging) {
        log(data, assert_4,
          "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']",
          item[0], 'alternativeTitles')
      }
    })
  })

  let assert_5 = 'Should match 246 to alternativeTitles when there is also 130'
  it(assert_5, function () {
    let data = fs.readFileSync('spec/dataconverter/test4.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: "Cahiers d'urbanisme et d'aménagement du territoire" }
      )
      if (logging) {
        log(data, assert_5,
          "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']",
          item[0], 'alternativeTitles')
      }
    })
  })

  let assert_6 = 'Should match 222 to alternativeTitles (when there are also 130 and 246)'
  it(assert_6, function () {
    let data = fs.readFileSync('spec/dataconverter/test4.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: "Cahiers d'urbanisme et d'aménagement du territoire" })
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: "Les cahiers d'urbanisme" })
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Urbana tidskrifter' })
      if (logging) {
        log(data,
          assert_6,
          "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']",
          item[0], 'alternativeTitles')
      }
    })
  })

  // EDITIONS
  let assert_7 = 'Should add editions (250) to the editions list and enforce unique'
  it(assert_7, function () {
    let data = fs.readFileSync('spec/dataconverter/test_editions.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].editions).toContain('8. uppl.')
      expect(item[0].editions).toContain('[Revised]')
      if (logging) {
        log(data, assert_7, "//marc:datafield[@tag='250']", item[0], 'editions')
      }
    })
  })

  // LANGUAGES
  let assert_8 = 'Should add languages (041$a) to the languages list; ignores non-ISO languages'
  it(assert_8, function () {
    let data = fs.readFileSync('spec/dataconverter/test_multiple_languages.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].languages).toContain('eng')
      expect(item[0].languages).toContain('ger')
      expect(item[0].languages).toContain('fre')
      expect(item[0].languages).toContain('ita')
      expect(item[0].languages).not.toContain('en_US')
      expect(item[0].languages).not.toContain('###')
      expect(item[0].languages).not.toContain('zxx')
      if (logging) {
        log(data, assert_8, "//marc:datafield[@tag='041']", item[0], 'languages')
      }
    })
  })

  let assert_9 = 'Should add language found in 008 where there is no 041'
  it(assert_9, function () {
    let data = fs.readFileSync('spec/dataconverter/test_language_in_008.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].languages).toContain('fre')
      if (logging) {
        log(data, assert_9, "//marc:controlfield[@tag='008']", item[0], 'languages')
      }
    })
  })

  // HRID
  let assert_10 = 'Should create an hrid using the OAI-PMH identifier'
  it(assert_10, function () {
    let data = fs.readFileSync('spec/dataconverter/test2.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].hrid).toBe('https://libris.kb.se/6qjvp1hj3xk33vg')
      if (logging) {
        log(data, assert_10, '/oai:OAI-PMH/oai:ListRecords/oai:record/oai:header', item[0], 'hrid')
      }
    })
  })

  // PHYSICAL DESCRIPTIONS
  let assert_11 = 'Should add physical descriptions (300$abce)'
  it(assert_11, function () {
    let data = fs.readFileSync('spec/dataconverter/test_physical_descriptions.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].physicalDescriptions).toContain('xxxiv, 416 pages illustrations 24 cm.')
      if (logging) {
        log(data, assert_11, "//marc:datafield[@tag='300']", item[0], 'physicalDescriptions')
      }
    })
  })

  // INDEX TITLE (245)
  let assert_12 = 'Should trim title (245) by n-chars, as specified by indicator 2'
  it(assert_12, function () {
    let data = fs.readFileSync('spec/dataconverter/test_index_title.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      expect(item[0].indexTitle).toBe('cahiers d\'urbanisme')
      if (logging) {
        log(data, assert_12, "//marc:datafield[@tag='245']", item[0], 'indexTitle')
      }
    })
  })

  // ALTERNATIVE TITLES: 130, 222, 240, 246, 247
  let assert_13 = 'Should add all types of alternative titles: 130, 222, 240, 246, 247 '
  it(assert_13, function () {
    let data = fs.readFileSync('spec/dataconverter/test_alternative_titles.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 246
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Engineering identities, epistemologies and values - remainder title' }
      )
      // 247
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Medical world news annual review of medicine' }
      )
      // 240
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Laws, etc. (Laws of Kenya : 1948)' }
      )
      // 222
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Soviet astronomy letters' }
      )
      // 130
      expect(item[0].alternativeTitles).toContain(
        { alternativeTitle: 'Star is born (Motion picture : 1954)' }
      )
      if (logging) {
        log(data, assert_13,
          "//marc:datafield[@tag='130' or @tag='222' or @tag='240' or @tag='246' or @tag='247']",
          item[0], 'alternativeTitles')
      }
    })
  })

  // IDENTIFIERS: 010, 019, 020, 022, 024, 028, 035
  let assert_14 = 'Should add identifiers: 010, 019, 020, 022, 024, 028, 035'
  it(assert_14, function () {
    let data = fs.readFileSync('spec/dataconverter/test_identifiers.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // 010
      expect(item[0].identifiers).toContain(
        { value: '2008011507', identifierTypeId: 'c858e4f2-2b6b-4385-842b-60732ee14abb' }
      )
      // 020
      expect(item[0].identifiers).toContain(
        { value: '9780307264787', identifierTypeId: '8261054f-be78-422d-bd51-4ed9f33c3422' }
      )
      expect(item[0].identifiers).toContain(
        { value: '9780071842013', identifierTypeId: '8261054f-be78-422d-bd51-4ed9f33c3422' }
      )
      expect(item[0].identifiers).toContain(
        { value: '0071842012', identifierTypeId: '8261054f-be78-422d-bd51-4ed9f33c3422' }
      )
      // 020$z
      expect(item[0].identifiers).toContain(
        { value: '9780307264755', identifierTypeId: 'fc6a5089-d1ef-46d6-ab4b-cbbc15055a76' }
      )
      expect(item[0].identifiers).toContain(
        { value: '9780307264766', identifierTypeId: 'fc6a5089-d1ef-46d6-ab4b-cbbc15055a76' }
      )
      expect(item[0].identifiers).toContain(
        { value: '9780307264777', identifierTypeId: 'fc6a5089-d1ef-46d6-ab4b-cbbc15055a76' }
      )
      // 022
      expect(item[0].identifiers).toContain(
        { value: '0376-4583', identifierTypeId: '913300b2-03ed-469a-8179-c1092c991227' }
      )
      // 022$z
      expect(item[0].identifiers).toContain(
        { value: '0027-3473', identifierTypeId: 'c216c962-ae4b-4279-be09-9ee25fe04e05' }
      )
      expect(item[0].identifiers).toContain(
        { value: '0027-3475', identifierTypeId: 'c216c962-ae4b-4279-be09-9ee25fe04e05' }
      )
      expect(item[0].identifiers).toContain(
        { value: '0027-3476', identifierTypeId: 'c216c962-ae4b-4279-be09-9ee25fe04e05' }
      )
      // 022$l
      expect(item[0].identifiers).toContain(
        { value: '1234-1232', identifierTypeId: 'c2972b6b-1616-4803-9acd-958ddf855928' }
      )
      // 02$2m
      expect(item[0].identifiers).toContain(
        { value: '1560-15605', identifierTypeId: '7d437a09-d000-41c5-8ac9-888798a753ca' }
      )
      // 022$y
      expect(item[0].identifiers).toContain(
        { value: '0046-2254', identifierTypeId: 'c5b2d7e9-f523-41ba-938b-6651b65de522' }
      )
      // 024
      expect(item[0].identifiers).toContain(
        { value: '7822183031', identifierTypeId: '2e8b3b6c-0e7d-4e48-bca2-b0b23b376af5' }
      )
      expect(item[0].identifiers).toContain(
        { value: 'M011234564', identifierTypeId: '2e8b3b6c-0e7d-4e48-bca2-b0b23b376af5' }
      )
      // 028
      expect(item[0].identifiers).toContain(
        { value: 'PJC 222013', identifierTypeId: 'b5d8cdc4-9441-487c-90cf-0c7ec97728eb' }
      )
      // 035
      expect(item[0].identifiers).toContain(
        { value: '(OCoLC)898162644', identifierTypeId: '7e591197-f335-4afb-bc6d-a6d76ca3bace' }
      )
      // 035$z
      expect(item[0].identifiers).toContain(
        { value: '(OCoLC)898087359', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      expect(item[0].identifiers).toContain(
        { value: '(OCoLC)930007675', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      expect(item[0].identifiers).toContain(
        { value: '(OCoLC)942940565', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      // 019
      expect(item[0].identifiers).toContain(
        { value: '62874189', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      expect(item[0].identifiers).toContain(
        { value: '244170452', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      expect(item[0].identifiers).toContain(
        { value: '677051564', identifierTypeId: '1090fc18-ee8e-4503-8187-3c4125d9e214' }
      )
      if (logging) {
        log(data, assert_14,
          "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']",
          item[0], 'identifiers')
      }
    })
  })

  // SERIES: 800, 810, 811, 830, 440, 490
  let assert_15 = 'Should add series statements (800, 810, 811, 830, 440, 490) to series list'
  it(assert_15, function () {
    let data = fs.readFileSync('spec/dataconverter/test_series.xml', 'utf8')
    return dataConverter.convertMarcToFolio(data).then((item) => {
      expect(item[0].isValid).toBeTruthy()
      // console.log('ITEM', item[0].toJSON());
      // 800
      expect(item[0].series).toContain('Joyce, James, 1882-1941. James Joyce archive.')
      // 810
      expect(item[0].series).toContain('United States. Dept. of the Army. Field manual.')
      // 811
      expect(item[0].series).toContain('International Congress of Nutrition (11th : 1978 : Rio de Janeiro, Brazil). Nutrition and food science ; v. 1.')
      // 830
      expect(item[0].series).toContain('Philosophy of engineering and technology ; v. 21.')
      expect(item[0].series).toContain('American university studies. Foreign language instruction ; vol. 12.')
      // 440
      expect(item[0].series).toContain('Journal of polymer science. Part C, Polymer symposia ; no. 39')
      // 490
      expect(item[0].series).toContain('Pediatric clinics of North America ; v. 2, no. 4.')
    })
    if (logging) {
      log(data, assert_15,
        "//marc:datafield[@tag='800' or @tag='810' or @tag='830' or @tag='440' or @tag='490' or @tag='811']",
        item[0], 'series')
    }
  })

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
