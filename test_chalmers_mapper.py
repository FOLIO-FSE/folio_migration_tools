import unittest
import pymarc
import json
from lxml import etree
from collections import namedtuple
from jsonschema import validate
from marc_to_folio.chalmers_mapper import ChalmersMapper
from marc_to_folio.folio_client import FolioClient


class TestChalmersMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open('./tests/test_config.json') as settings_file:
            cls.config = json.load(settings_file,
                                   object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            cls.folio = FolioClient(cls.config)
            cls.mapper = ChalmersMapper(cls.folio)
            cls.instance_schema = cls.folio.get_instance_json_schema()

    def do_map(self, file_name, xpath, message):
        ns = {'marc': 'http://www.loc.gov/MARC21/slim',
              'oai': 'http://www.openarchives.org/OAI/2.0/'}
        file_path = r'./tests/test_data/chalmers/{}'.format(file_name)
        record = pymarc.parse_xml_to_array(file_path)[0]
        result = self.mapper.parse_bib(record, "source")
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


if __name__ == '__main__':
    unittest.main()
