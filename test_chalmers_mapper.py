import unittest
import pymarc
import json
from lxml import etree
from collections import namedtuple
from jsonschema import validate
from marc_to_folio.chalmers_mapper import ChalmersMapper
from folioclient.FolioClient import FolioClient


class TestChalmersMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open('./tests/test_config.json') as settings_file:
            cls.config = json.load(settings_file,
                                   object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            cls.folio = FolioClient(cls.config.okapi_url,
                                    cls.config.tenant_id,
                                    cls.config.username,
                                    cls.config.password)
            cls.mapper = ChalmersMapper(cls.folio, '')
            cls.instance_schema = cls.folio.get_instance_json_schema()

    def do_map(self, file_name, xpath, message):
        ns = {'marc': 'http://www.loc.gov/MARC21/slim',
              'oai': 'http://www.openarchives.org/OAI/2.0/'}
        file_path = r'./tests/test_data/chalmers/{}'.format(file_name)
        record = pymarc.parse_xml_to_array(file_path)[0]
        result = self.mapper.parse_bib(record, "source")
        if self.config.validate_json_schema:
            validate(result, self.instance_schema)
        root = etree.parse(file_path)
        data = str('')
        for element in root.xpath(xpath, namespaces=ns):
            data = ' '.join(
                [data, str(etree.tostring(element, pretty_print=True), 'utf-8')])
        return [result, message + '\n' + data]

    def test_simple_title(self):
        message = 'A simple title should not contain contributor info nor /'
        xpath = "//marc:datafield[@tag='245']"
        record = self.do_map('test1.xml', xpath, message)
        self.assertEqual('Modern Electrosynthetic Methods in Organic Chemistry', record[0]['title'],
                         record[1])
        self.assertNotIn('/', record[0]['title'], record[1])
        self.assertEqual('6312d172-f0cf-40f6-b27d-9fa8feaf332f',
                         record[0]['instanceTypeId'])
        self.assertEqual('9d18a02f-5897-4c31-9106-c9abb5c7ae8b',
                         record[0]['modeOfIssuanceId'])

    def test_simple_title_two245s(self):
        message = 'With two 245s, the longes should be choosen'
        xpath = "//marc:datafield[@tag='245']"
        record = self.do_map('test_title_with_two_245s.xml', xpath, message)
        self.assertEqual('Raspberry Pi with Java : programming the internet of things (IoT)', record[0]['title'],
                         record[1])

    def test_composed_title(self):
        message = 'Should create a composed title (245) with the [a, b, k, n, p] subfields.'
        xpath = "//marc:datafield[@tag='245']"
        record = self.do_map('test_composed_title.xml', xpath, message)
        # self.assertFalse('/' in record['title'])
        self.assertEqual('The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment.',
                         record[0]['title'], record[1])

    def test_contributors(self):
        message = 'Should add contributors (100, 111 700) to the contributors list'
        xpath = "//marc:datafield[@tag='100' or @tag='111' or @tag='700']"
        record = self.do_map('test_contributors.xml', xpath, message)
        contributors = list((c['name'] for c in record[0]['contributors']))
        m = message + '\n' + record[1]
        with self.subTest("100, no contrib type indicated"):
            self.assertIn('Chin, Stephen, 1977-', contributors, m)
        with self.subTest("100$4"):
            self.assertIn('Presthus, Robert Vance', contributors, m)
        with self.subTest("100$ade4, unknown typeid, set type text to cartographer"):
            self.assertIn('Lous, Christian Carl, 1724-1804', contributors, m)
        with self.subTest("700$e (contributor)"):
            self.assertIn('Weaver, James L.', contributors, m)
        with self.subTest("111$acde, no contrib type id"):
            self.assertIn('Wolfcon Durham 2018', contributors, m)
        with self.subTest("111$abbde4"):
            self.assertIn(
                'Kyōto Daigaku. Genshiro Jikkenjo. Senmon Kenkyūkai (2013 January 25)', contributors, m)
        with self.subTest("111$aee44  multiple relation types (author, illustrator), pick first one?"):
            self.assertIn('Tupera Tupera (Firm)', contributors, m)

    def test_ids(self):
        message = 'Should fetch Libris Bib id, Libris XL id and Sierra ID'
        xpath = "//marc:datafield[@tag='001' or @tag='907' or @tag='887']"
        record = self.do_map('test_publications.xml', xpath, message)
        bibid = {'identifierTypeId': '28c170c6-3194-4cff-bfb2-ee9525205cf7',
                 'value': '21080448'}
        self.assertIn(bibid, record[0]['identifiers'], record[1])
        xl_id = {'identifierTypeId': '925c7fb9-0b87-4e16-8713-7f4ea71d854b',
                 'value': 'https://libris.kb.se/8sl08b9l54wxk4m'}
        self.assertIn(xl_id, record[0]['identifiers'], record[1])
        sierra_id = {'identifierTypeId': '5fc83ef4-7572-40cf-9f64-79c41e9ccf8b',
                     'value': '0000001'}
        self.assertIn(sierra_id, record[0]['identifiers'], record[1])

    def test_old_bib_ids(self):
        message = 'Should fetch Libris Bib id,'
        xpath = "//marc:datafield[@tag='001' or @tag='907' or @tag='887']"
        record = self.do_map(
            'test_identifiers_libris_old_bib.xml', xpath, message)
        bibid = {'identifierTypeId': '28c170c6-3194-4cff-bfb2-ee9525205cf7',
                 'value': '21080448'}
        self.assertIn(bibid, record[0]['identifiers'], record[1])
        xl_id = {'identifierTypeId': '925c7fb9-0b87-4e16-8713-7f4ea71d854b',
                 'value': 'http://libris.kb.se/bib/21080448'}
        sierra_id = {'identifierTypeId': '5fc83ef4-7572-40cf-9f64-79c41e9ccf8b',
                     'value': '0000001'}
        self.assertIn(sierra_id, record[0]['identifiers'], record[1])
        # self.assertIn(xl_id, record[0]['identifiers'], record[1])

    def test_permanent_location_two_holdings(self):
        message = 'PERMANENT LOCATION, TWO HOLDINGS'
        xpath = "//marc:datafield[@tag='001' or @tag='866' or @tag='852']"
        self.mapper.holdings_map = {}
        record = self.do_map('multiple_852s.xml', xpath, message)
        self.assertEqual(2, len(self.mapper.holdings_map))
        permanenent_loc_ids = [h["permanentLocationId"]
                               for h in self.mapper.holdings_map.values()]
        callNumbers = [h["callNumber"]
                       for h in self.mapper.holdings_map.values()]
        holdingsStatements = [h["holdingsStatements"][0]
                              for h in self.mapper.holdings_map.values()]
        self.assertIn("e2e4b00a-fbe7-4c2a-ac50-361062949d56",
                      permanenent_loc_ids)
        self.assertIn("921e0666-fdd3-4e54-a4c4-a20d8d2333fd",
                      permanenent_loc_ids)
        self.assertIn("Vp", callNumbers)
        self.assertIn("Sjöfartstidskrifter", callNumbers)
        self.assertIn({'statement': 'Årg. 24-40 (1986-2002)',
                       'note': ''}, holdingsStatements)
        # self.assertIn("", holdingsStatements)

    def test_inventory_only(self):
        message = 'PERMANENT LOCATION, TWO HOLDINGS'
        xpath = "//marc:datafield[@tag='001' or @tag='887' or @tag='907']"
        self.mapper.holdings_map = {}
        record = self.do_map('inventory_only.xml', xpath, message)
        print(record[0]['identifiers'])
        self.assertTrue(True)

    def test_identifiers(self):
        message = 'Should add identifiers: 010, 019, 020, 022, 024, 028, 035 and local IDs'
        xpath = "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']"
        record = self.do_map('test_identifiers_libris.xml', xpath, message)
        m = message + '\n' + record[1]
        # TODO: Test identifier type id in additional mappers
        print(record[0]['publication'])
        identifier_values = (i['value']
                                     for i in record[0]['identifiers'])
        self.assertIn('2008011507', identifier_values, m)
        self.assertIn('9780307264787', identifier_values, m)
        self.assertIn('9780071842013', identifier_values, m)
        self.assertIn('0071842012', identifier_values, m)
        self.assertIn('9780307264755', identifier_values, m)
        self.assertIn('9780307264766', identifier_values, m)
        self.assertIn('9780307264777', identifier_values, m)
        self.assertIn('0376-4583', identifier_values, m)
        self.assertIn('0027-3475', identifier_values, m)
        self.assertIn('0027-3476', identifier_values, m)
        self.assertIn('1234-1232', identifier_values, m)
        self.assertIn('1560-15605', identifier_values, m)
        self.assertIn('0046-2254', identifier_values, m)
        self.assertIn('7822183031', identifier_values, m)
        self.assertIn('M011234564', identifier_values, m)
        self.assertIn('PJC 222013', identifier_values, m)
        self.assertIn('(OCoLC)898162644', identifier_values, m)
        self.assertIn('(OCoLC)898087359', identifier_values, m)
        self.assertIn('(OCoLC)930007675', identifier_values, m)
        self.assertIn('(OCoLC)942940565', identifier_values, m)
        self.assertIn('0027-3473', identifier_values, m)
        self.assertIn('62874189', identifier_values, m)
        self.assertIn('244170452', identifier_values, m)
        self.assertIn('677051564', identifier_values, m)
        bibid = {
            'identifierTypeId': '28c170c6-3194-4cff-bfb2-ee9525205cf7',
            'value': '21080448'}
        self.assertIn(bibid, record[0]['identifiers'], record[1])
        xl_id = {'identifierTypeId': '925c7fb9-0b87-4e16-8713-7f4ea71d854b',
                 'value': 'https://libris.kb.se/8sl08b9l54wxk4m'}
        self.assertIn(xl_id, record[0]['identifiers'], record[1])
        sierra_id = {
            'identifierTypeId': '5fc83ef4-7572-40cf-9f64-79c41e9ccf8b',
            'value': '0000001'}
        self.assertIn(sierra_id, record[0]['identifiers'], record[1])
        xl_id_short = {
            'identifierTypeId': '4f3c4c2c-8b04-4b54-9129-f732f1eb3e14',
            'value': '8sl08b9l54wxk4m'}
        self.assertIn(xl_id_short, record[0]['identifiers'], record[1])


if __name__ == '__main__':
    unittest.main()
