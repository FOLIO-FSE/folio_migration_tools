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
            cls.mapper = ChalmersMapper(cls.folio)
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

    def test_composed_title(self):
        message = 'Should create a composed title (245) with the [a, b, k, n, p] subfields.'
        xpath = "//marc:datafield[@tag='245']"
        record = self.do_map('test_composed_title.xml', xpath, message)
        # self.assertFalse('/' in record['title'])
        self.assertEqual('The wedding collection. Volume 4, Love will be our home: 15 songs of love and commitment.',
                         record[0]['title'], record[1])

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
        record = self.do_map('test_identifiers_libris_old_bib.xml', xpath, message)
        bibid = {'identifierTypeId': '28c170c6-3194-4cff-bfb2-ee9525205cf7',
                 'value': '21080448'}
        self.assertIn(bibid, record[0]['identifiers'], record[1])
        xl_id = {'identifierTypeId': '925c7fb9-0b87-4e16-8713-7f4ea71d854b',
                 'value': 'https://libris.kb.se/bib/21080448'}
        self.assertIn(xl_id, record[0]['identifiers'], record[1])
        sierra_id = {'identifierTypeId': '5fc83ef4-7572-40cf-9f64-79c41e9ccf8b',
                     'value': '0000001'}
        self.assertIn(sierra_id, record[0]['identifiers'], record[1])

    def test_identifiers(self):
        message = 'Should add identifiers: 010, 019, 020, 022, 024, 028, 035 and local IDs'
        xpath = "//marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='019']"
        record = self.do_map('test_identifiers_libris.xml', xpath, message)
        m = message + '\n' + record[1]
        # TODO: Test identifier type id in additional mappers
        self.assertIn('2008011507', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('9780307264787', (i['value']
                                        for i in record[0]['identifiers']), m)
        self.assertIn('9780071842013', (i['value']
                                        for i in record[0]['identifiers']), m)
        self.assertIn('0071842012', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('9780307264755', (i['value']
                                        for i in record[0]['identifiers']), m)
        self.assertIn('9780307264766', (i['value']
                                        for i in record[0]['identifiers']), m)
        self.assertIn('9780307264777', (i['value']
                                        for i in record[0]['identifiers']), m)
        self.assertIn('0376-4583', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('0027-3475', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('0027-3476', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('1234-1232', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('1560-15605', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('0046-2254', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('7822183031', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('M011234564', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('PJC 222013', (i['value']
                                     for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)898162644', (i['value']
                                           for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)898087359', (i['value']
                                           for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)930007675', (i['value']
                                           for i in record[0]['identifiers']), m)
        self.assertIn('(OCoLC)942940565', (i['value']
                                           for i in record[0]['identifiers']), m)
        self.assertIn('0027-3473', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('62874189', (i['value']
                                   for i in record[0]['identifiers']), m)
        self.assertIn('244170452', (i['value']
                                    for i in record[0]['identifiers']), m)
        self.assertIn('677051564', (i['value']
                                    for i in record[0]['identifiers']), m)
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
