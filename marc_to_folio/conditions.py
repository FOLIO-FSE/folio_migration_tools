
import re


class Conditions():

    def __init__(self, folio):
        self.folio = folio
        self.instance_note_types = {}
        self.contrib_name_types = {}
        self.electronic_access_relationships = {}

    def get_condition(self, name, value, parameter=None, marc_field=None):
        try:
            return getattr(self, 'condition_' + str(name))(value, parameter, marc_field)
        except AttributeError as attrib_error:
            print(f'Unhandled condition: {name} {attrib_error} ')
            return value
        except ValueError as value_error:
            print(f'Unhandled value: {name} {value_error} {type(value_error)} ')
            return value

    def condition_trim_period(self, value, parameter, marc_field):
        return value.strip().rstrip('.').rstrip(',')

    def condition_trim(self, value, parameter, marc_field):
        return value.strip()

    def condition_remove_ending_punc(self, value, parameter, marc_field):
        v = value
        chars = '.;:,/+=- '
        while len(v) > 0 and v[-1] in chars:
            v = v.rstrip(v[-1])
        return v

    def condition_remove_prefix_by_indicator(self, value, parameter, marc_field):
        '''Returns the index title according to the rules'''
        ind2 = marc_field.indicator2
        reg_str = r'[\s:\/]{0,3}$'
        if ind2 in map(str, range(1, 9)):
            num_take = int(ind2)
            return re.sub(reg_str, '', value[num_take:])
        else:
            return re.sub(reg_str, '', value)

    def condition_capitalize(self, value, parameter, marc_field):
        return value.capitalize()

    def condition_clean_isbn(self, value, parameter, marc_field):
        return value

    def condition_set_publisher_role(self, value, parameter, marc_field):
        roles = {"0": "Production",
                 "1": "Publication",
                 "2": "Distribution",
                 "3": "Manufacture"}

        return roles.get(marc_field.indicator2, '')

    def condition_set_identifier_type_id_by_value(self, value, parameter, marc_field):
        v = next((f['id'] for f in self.folio.identifier_types
                  if f['name'] in parameter['names']), '')
        if not v:
            raise ValueError(
                f'no identifier_types specified {marc_field}')
        return v

    def condition_set_note_type_id(self, value, parameter, marc_field):
        if not any(self.instance_note_types):
            self.instance_note_types = self.folio.folio_get_all("/instance-note-types",
                                                                "instanceNoteTypes",
                                                                '?query=cql.allRecords=1 sortby name')
        v = next((f['id'] for f
                  in self.instance_note_types
                  if f['name'] == parameter['name']), '')
        if not v:
            raise ValueError(
                f'no instance_note_types specified {marc_field}')
        return v

    def condition_set_classification_type_id(self, value, parameter, marc_field):
        # undef = next((f['id'] for f in self.folio.class_types
        #             if f['name'] == 'No type specified'), '')
        v = next((f['id'] for f in self.folio.class_types
                  if f['name'] == parameter['name']), '')
        if not v:
            raise ValueError(
                f'no classification_type specified {marc_field}')
        return v

    def condition_char_select(self, value, parameter, marc_field):
        return value[parameter['from']: parameter['to']]

    def condition_set_identifier_type_id_by_name(self, value, parameter, marc_field):
        v = next((f['id'] for f
                  in self.folio.identifier_types
                  if f['name'] == parameter['name']), '')
        if not v:
            raise ValueError(
                f'no identifier_type_id specified {marc_field}')
        return v

    def condition_set_contributor_name_type_id(self, value, parameter, marc_field):
        if not any(self.contrib_name_types):
            self.contrib_name_types = {f['name']: f['id']
                                       for f in self.folio.contrib_name_types}
        return self.contrib_name_types[parameter['name']]

    def condition_set_contributor_type_id(self, value, parameter, marc_field):
        undefined = {}
        for cont_type in self.folio.contributor_types:
            if cont_type['name'] == 'Undefined':
                undefined = cont_type
            for subfield in marc_field.get_subfields('4', 'e'):
                if subfield.lower() == cont_type['code'].lower():
                    return cont_type['id']
                if subfield.lower() == cont_type['name'].lower():
                    return cont_type['id']
        return undefined['id']

    def condition_set_contributor_type_text(self, value, parameter, marc_field):
        undefined = {}
        for cont_type in self.folio.contributor_types:
            if cont_type['name'] == 'Undefined':
                undefined = cont_type
            for subfield in marc_field.get_subfields('4', 'e'):
                if subfield.lower() == cont_type['code'].lower():
                    return cont_type['name']
                elif subfield.lower() == cont_type['name'].lower():
                    return cont_type['name']
        return undefined['name']

    def condition_set_alternative_title_type_id(self, value, parameter, marc_field):
        v = next((f['id'] for f in self.folio.alt_title_types
                  if f['name'] == parameter['name']), '')
        if not v:
            raise ValueError(
                f'no alt_title_types specified {marc_field}')
        return v

    def condition_remove_substring(self, value, parameter, marc_field):
        return value.replace(parameter['substring'], '')

    def condition_set_instance_type_id(self, value, parameter, marc_field):
        v = next((f['id'] for f in self.folio.instance_types
                  if f['code'] == value), '')
        if not v:
            raise ValueError(
                f'no instance_types specified {marc_field}')
        return v

    def condition_set_instance_format_id(self, value, parameter, marc_field):
        v = next((f['id'] for f in self.folio.instance_formats
                  if f['code'] == value), '')
        if not v:
            raise ValueError(
                f'no instance_formats specified {marc_field}')
        return v

    def condition_set_electronic_access_relations_id(self, value, parameter, marc_field):
        enum = {'0': "resource",
                '1': "version of resource",
                '2': "related resource",
                '3': "no information provided"}
        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum['3'])
        if not any(self.electronic_access_relationships):
            self.electronic_access_relationships = self.folio.folio_get_all("/electronic-access-relationships",
                                                                            "electronicAccessRelationships",
                                                                            '?query=cql.allRecords=1 sortby name')

        v = next((f['id'] for f
                  in self.electronic_access_relationships
                  if f['name'].lower() == name), '')
        if not v:
            raise ValueError(
                f'no electronic_access_relationships specified {marc_field}')
        return v
