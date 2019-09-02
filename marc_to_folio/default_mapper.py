'''The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications'''
import uuid
import re
import xml.etree.ElementTree as ET

import requests


class DefaultMapper:
    '''Maps a MARC record to inventory instance format according to
    the FOLIO community convention'''
    # Bootstrapping (loads data needed later in the script.)
    def __init__(self, folio):
        self.filter_chars = r'[.,\/#!$%\^&\*;:{}=\-_`~()]'
        self.filter_last_chars = r',$'
        self.folio = folio
        print("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())

    def parse_bib(self, marc_record, record_source):
        ''' Parses a bib recod into a FOLIO Inventory instance object
            Community mapping suggestion: https://bit.ly/2S7Gyp3
             This is the main function'''
        # not mapped. Randomizes an instance type:
        rec = {
            'id': str(uuid.uuid4()),
            # This should be the new Libris ID?
            'hrid': str(marc_record['001'].format_field()),
            # TODO: add Instance status
            'title': self.get_title(marc_record),
            'indexTitle': self.get_index_title(marc_record),
            'source': record_source,
            'contributors': list(self.get_contributors(marc_record)),
            'identifiers': list(self.get_identifiers(marc_record)),
            # TODO: Add instanceTypeId
            'instanceTypeId': self.folio.instance_types[0]['id'],
            'alternativeTitles': list(self.get_alt_titles(marc_record)),
            'publicationFrequency': list(set(self.get_publication_frequency(marc_record))),
            'publicationRange': list(set(self.get_publication_range(marc_record))),
            'series': list(set(self.get_series(marc_record))),
            'editions': list(set(self.get_editions(marc_record))),
            'subjects': list(set(self.get_subjects(marc_record))),
            'classifications': list(self.get_classifications(marc_record)),
            'publication': list((self.get_publication(marc_record))),
            # TODO: add instanceFormatId
            'instanceFormatIds': [self.folio.instance_formats[0]['id']],
            # TODO: Mode of issuance
            # TODO: add physical description
            'physicalDescriptions': list(self.get_physical_desc(marc_record)),
            'languages': self.get_languages(marc_record),
            'notes': list(self.get_notes(marc_record))}
        return rec

    def get_editions(self, marc_record):
        fields = marc_record.get_fields('250')
        for field in fields:
            yield " ".join(field.get_subfields('a', 'b'))

    def get_publication_frequency(self, marc_record):
        for tag in ['310', '321']:
            for field in marc_record.get_fields(tag):
                yield ' '.join(field.get_subfields(*'ab'))

    def get_publication_range(self, marc_record):
        for field in marc_record.get_fields('362'):
            yield ' '.join(field.get_subfields('a'))

    def get_physical_desc(self, marc_record):
        # TODO: improve according to spec
        for tag in ['300']:
            for field in marc_record.get_fields(tag):
                yield field.format_field()

    def get_index_title(self, marc_record):
        # TODO: fixa!
        '''Returns the index title according to the rules'''
        if '245' not in marc_record:
            return ''
        field = marc_record['245']
        title_string = " ".join(field.get_subfields('a', 'n', 'p'))
        ind2 = field.indicator2
        if ind2 == '0':
            return title_string
        elif ind2 in map(str, range(1, 9)):
            num_take = int(ind2)
            return title_string[num_take:]
        else:
            return ''

    def get_notes(self, marc_record):
        '''Collects all notes fields and stores them as generic notes.'''
        # TODO: specify note types with better accuracy.
        note_tags = {'500': 'a35',
                     '501': 'a5',
                     '502': 'abcd',
                     '504': 'ab',
                     '505': 'agrt',
                     '506': 'a',
                     '507': 'ab',
                     '508': 'a',
                     '510': 'abcx',
                     '511': 'a',
                     '513': 'ab',
                     '514': 'acdeghz',
                     '515': 'a',
                     '516': 'a',
                     '518': '3adop',
                     '520': '3abc',
                     '522': 'a',
                     '524': 'a',
                     '525': 'a',
                     '530': 'a',
                     '532': 'a',
                     '533': 'abcdefmn35',
                     '534': 'abcefklmnoptxz3',
                     '540': 'abcdu5',
                     '541': '3abcdefhno5',
                     '542': 'abcdngfosu',
                     '544': 'ad',
                     '545': 'abu',
                     '546': '3ab',
                     '547': 'a',
                     '550': 'a',
                     '552': 'ablmnz',
                     '555': 'abcdu',
                     '556': 'az',
                     '561': '3au5',
                     '562': '3abc5',
                     '563': '3a5',
                     '565': 'a',
                     '567': 'a',
                     '580': 'a',
                     '583': 'abcdefhijklnouxz235',
                     '586': 'a',
                     '590': 'a',
                     '592': 'a',
                     '599': 'abcde'}
        for key, value in note_tags.items():
            for field in marc_record.get_fields(key):
                yield {
                    # TODO: add logic for noteTypeId
                    "instanceNoteTypeId": "9d4fcaa1-b1a5-48ea-b0d1-986839737ad2",
                    "note": " ".join(field.get_subfields(*value)),
                    # TODO: Map staffOnly according to field
                    "staffOnly": False
                } 

    def get_title(self, marc_record):
        if '245' not in marc_record:
            return ''
        '''Get title or raise exception.'''
        title = " ".join(marc_record['245'].get_subfields(*list('anpbcfghks')))
        if title:
            return title
        else:
            raise ValueError("No title for {}\n{}"
                             .format(marc_record['001'], marc_record))

    def folio_record_template(self, identifier):
        '''Create a new folio record from template'''
        # if created from json schema validation could happen earlier...
        return {'id': str(identifier)}

    def get_contributors(self, marc_record):
        '''Collects contributors from the marc record and adds the apropriate
        Ids'''
        def name_type_id(n): return next((f['id'] for f
                                          in self.folio.contrib_name_types
                                          if f['name'] == n), '')
        fields = {'100': {'subfields': 'abcdq',
                          'nameTypeId': 'Personal name'},
                  '110': {'subfields': 'abcdn',
                          'nameTypeId': 'Corporate name'},
                  '111': {'subfields': 'abcd',
                          'nameTypeId': 'Meeting name'},
                  '700': {'subfields': 'abcdq',
                          'nameTypeId': 'Personal name'},
                  '710': {'subfields':  'abcdn',
                          'nameTypeId': 'Corporate name'},
                  '711': {'subfields': 'abcd',
                          'nameTypeId': 'Meeting name'}
                  }
        first = 0
        for field_tag in fields:
            for field in marc_record.get_fields(field_tag):
                ctype = self.get_contrib_type_id(marc_record)
                first += 1
                subs = field.get_subfields(*fields[field_tag]['subfields'])
                ntd = name_type_id(fields[field_tag]['nameTypeId'])
                yield {'name': re.sub(self.filter_last_chars, '', ' '.join(subs)),
                       'contributorNameTypeId': ntd,
                       'contributorTypeId': ctype,
                       'primary': first < 2}

    def get_contrib_type_id(self, marc_record):
        ''' Maps type of contribution to the right FOLIO Contributor types'''
        # TODO: create logic here...
        ret = '5daa3848-958c-4dd8-9724-b7ae83a99a27'
        return ret

    def get_urls(self, marc_record):
        for field in marc_record.get_fields('856'):
            yield {
                'uri': (field['u'] if 'u' in field else ''),
                'linkText': (field['y'] if 'y' in field else ''),
                'materialsSpecification': field['3'] if '3' in field else '',
                'publicNote': field['z'] if 'z' in field else ''
                # 'relationsshipId': field.indicator2}
            }

    def get_subjects(self, marc_record):
        ''' Get subject headings from the marc record.'''
        tags = {'600': 'abcdq',
                '610': 'abcdn',
                '611': 'acde',
                '630': 'adfhklst',
                '647': 'acdvxyz',
                '648': 'avxyz',
                '650': 'abcdvxyz',
                '651': 'avxyz'}
        non_mapped_tags = {'653': '',
                           '654': '',
                           '655': '',
                           '656': '',
                           '657': '',
                           '658': '',
                           '662': ''}
        for tag in list(non_mapped_tags.keys()):
            if any(marc_record.get_fields(tag)):
                print("Unmapped Subject field {} in {}"
                      .format(tag, marc_record['001']))
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield " ".join(field.get_subfields(*value)).strip()

    def get_alt_titles(self, marc_record):
        '''Finds all Alternative titles.'''
        fields = {'130': [next(f['id'] for f
                               in self.folio.alt_title_types
                               if f['name'] == 'No type specified'),
                          list('anpdfghklmorst')],
                  '222': [next(f['id'] for f
                               in self.folio.alt_title_types
                               if f['name'] == 'No type specified'),
                          list('anpdfghklmorst')],
                  '240': [next(f['id'] for f
                               in self.folio.alt_title_types
                               if f['name'] == 'No type specified'),
                          list('anpdfghklmors')],
                  '246': [next(f['id'] for f
                               in self.folio.alt_title_types
                               if f['name'] == 'No type specified'),
                          list('anpbfgh5')],
                  '247': [next(f['id'] for f
                               in self.folio.alt_title_types
                               if f['name'] == 'No type specified'),
                          list('anpbfghx')]}
        for field_tag in fields:
            for field in marc_record.get_fields(field_tag):
                yield {'alternativeTitleTypeId': fields[field_tag][0],
                       'alternativeTitle': " - "
                       .join(field.get_subfields(*fields[field_tag][1]))}
        # return list(dict((v['alternativeTitleTypeId'], v)
        #                 for v in res).values())

    def get_publication(self, marc_record):
        # TODO: Improve with 008 and 260/4 $c
        '''Publication'''
        for field in marc_record.get_fields('260'):
            yield {'publisher': re.sub(self.filter_chars, str(''), str(field['b'])).strip() or '',
                   'place': re.sub(self.filter_chars, str(''), str(field['a'])).strip() or '',
                   'dateOfPublication': re.sub(self.filter_chars, str(''), str(field['c'])).strip() or ''}
        for field in marc_record.get_fields('264'):
            yield {'publisher': re.sub(self.filter_chars, str(''), str(field['b'])).strip() or '',
                   'place': re.sub(self.filter_chars, str(''), str(field['a'])).strip() or '',
                   'dateOfPublication': re.sub(self.filter_chars, str(''), str(field['c'])).strip() or '',
                   'role': self.get_publication_role(field.indicators[1])}

    def get_publication_role(self, ind2):
        roles = {'0': 'Production',
                 '1': 'Publication',
                 '2': 'Distribution',
                 '3': 'Manufacturer',
                 '4': ''
                 }
        if ind2.strip() not in roles.keys():
            return roles['4']
        return roles[ind2.strip()]

    def get_series(self, marc_record):
        '''Series'''
        tags = {'440': 'anpv',
                '490': '3av',
                '800': 'abcdefghjklmnopqrstuvwx35',
                '810': 'abcdefghklmnoprstuvwx35',
                '811': 'acdefghjklnpqstuvwx35',
                '830': 'adfghklmnoprstvwx35'}
        for key, value in tags.items():
            for field in marc_record.get_fields(key):
                yield ' '.join(field.get_subfields(*value))

    def get_languages(self, marc_record):
        '''Get languages and tranforms them to correct codes'''
        languages = set()
        skip_languages = ['###', 'zxx']
        lang_fields = marc_record.get_fields('041')
        if len(lang_fields) > 0 :
            subfields = 'abdefghjkmn'
            for lang_tag in lang_fields:
                lang_codes = lang_tag.get_subfields(*list(subfields))
                for lang_code in lang_codes:
                    langlength = len(lang_code.replace(" ", ""))
                    if langlength == 3:
                        languages.add(lang_code.replace(" ", ""))
                    elif langlength > 3 and langlength % 3 == 0:
                        lc = lang_code.replace(" ", "")
                        new_codes = [lc[i:i+3]
                                     for i in range(0, len(lc), 3)]
                        languages.update(new_codes)
                        languages.discard(lang_code)

                languages.update()
            languages = set(self.filter_langs(filter(None, languages),
                                              skip_languages,
                                              marc_record['001'].format_field()))
        elif '008' in marc_record and len(marc_record['008'].data) > 38:
            from_008 = ''.join((marc_record['008'].data[35:38]))
            if from_008:
                languages.add(from_008)
        # TODO: test agianist valide language codes
        return list(languages)

    def fetch_language_codes(self):
        '''fetches the list of standardized language codes from LoC'''
        url = "https://www.loc.gov/standards/codelists/languages.xml"
        tree = ET.fromstring(requests.get(url).content)
        name_space = "{info:lc/xmlns/codelist-v1}"
        xpath_expr = "{0}languages/{0}language/{0}code".format(name_space)
        for code in tree.findall(xpath_expr):
            yield code.text

    def get_classifications(self, marc_record):
        '''Collects Classes and adds the appropriate metadata'''
        get_class_type_id = lambda x: next((f['id'] for f
                                           in self.folio.class_types
                                           if f['name'] == x), None)
        fields = {'050': ['LC', 'ab'],
                  '082': ['Dewey', 'a'],
                  '086': ['GDC', 'a'],
                  '090': ['LC', 'ab']
                  }
        for field_tag in fields:
            for field in marc_record.get_fields(field_tag):
                class_type = get_class_type_id(fields[field_tag][0])
                if class_type:
                    yield {'classificationTypeId': get_class_type_id(fields[field_tag][0]),
                           'classificationNumber': " ".join(field.get_subfields(*fields[field_tag][1]))}
                else:
                    print("No classification type for {} ({}) for {}."
                          .format(field.tag, field.format_field(),
                                  marc_record['001'].format_field(),))

    def get_identifiers(self, marc_record):
        '''Collects Identifiers and adds the appropriate metadata'''
        fields = {'010': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'LCCN'), ''), 'a'],
                  '019': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'System Control Number'), ''), 'a'],
                  '020': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'ISBN'), ''), 'acz'],
                  '024': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'Other Standard Identifier'), ''), 'a'],
                  '028': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'Publisher Number'), ''), 'a'],
                  '022': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'ISSN'), ''), 'a2zlmy'],
                  '035': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'Control Number'), ''), 'az'],
                  '074': [next((f['id'] for f
                                in self.folio.identifier_types
                                if f['name'] == 'GPO Item Number'), ''), 'a']
                  }
        for field_tag in fields:
            for field in marc_record.get_fields(field_tag):
                for subfield in field.get_subfields(*list(fields[field_tag][1])):
                    yield {'identifierTypeId': fields[field_tag][0],
                           'value': subfield}

    def filter_langs(self, language_values, forbidden_values, legacyid):
        for language_value in language_values:
            if language_value in self.language_codes and language_value not in forbidden_values:
                yield language_value
            else:
                if language_value == 'jap':
                    yield 'jpn'
                elif language_value == 'fra':
                    yield 'fre'
                else:
                    print('Illegal language code: {} for {}'
                    .format(language_value, legacyid))
