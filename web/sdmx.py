# This file is part of Lod4Stat.
#
# Copyright (C) 2014 Provincia autonoma di Trento
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import StringIO
import sdmx_message as sdmx
import time
from datetime import datetime
from l4s.settings import SENDER, LANGUAGE_CODE, SENDER_NAME
from web.utils import get_metadata_on_column,\
    get_data_from_data_frame,\
    get_column_description,\
    column_position_in_dataframe,\
    add_xml_header


class Item(object):
    """
    Structure used to represents internal triples;
    an hash table with subject key will point to this element.
    """
    t_predicate = ''
    t_object = ''

    def __init__(self, t_predicate=None, t_object=None):
        self.t_predicate = t_predicate
        self.t_object = t_object

    def get_t_predicate(self):
        return self.t_predicate

    def get_t_object(self):
        return self.t_object


def build_column_dict(data_frame, sql):
    """
    Build a dictionary with key column position and value the list of
    related items (predicate and objects).

    :param data_frame: The Pandas dataframe.
    :param sql: The sql query.
    :return:The column dictionary.
    """
    column_dict = dict()
    for line in sql.splitlines():
        left_stripped_line = line.lstrip(' ')
        words = left_stripped_line.split(' ')
        declare = words[0]
        declare_token = '--JOIN'
        if declare == declare_token:
            table_and_column = words[1]
            words = table_and_column.split('.')
            table_name = words[0]
            column_name = words[1]
            column_description = get_column_description(table_name,
                                                        column_name)
            index = column_position_in_dataframe(column_description,
                                                 data_frame)
            column_dict[index] = dict()
            column_dict[index]['table'] = table_name
            column_dict[index]['column'] = column_name

    col_dict = dict()

    for c, col in enumerate(data_frame.columns):
        if c not in column_dict:
            continue
        table_name = column_dict[c]['table']
        column_name = column_dict[c]['column']
        rows = get_metadata_on_column(table_name, column_name)
        for row in rows:
            t_predicate = "%s" % row[3]
            t_object = "%s" % row[4]
            if '#' in t_predicate:
                t_predicate = t_predicate.split('#')[1]
            else:
                continue
            if '#' in t_object:
                t_object = t_object.split('#')[1]
            else:
                continue

            item = Item(t_predicate.strip(), t_object.strip())
            if c in col_dict:
                col_dict[c].append(item)
            else:
                elements = [item]
                col_dict[c] = elements

    return col_dict


def get_concept(col_dict, c):
    """
    Return concept on column c.

    :param col_dict: Column dictionary.
    :param c: Column index.
    """
    if c not in col_dict:
        # If is not annotated I presume that it is an observable value.
        return "obsValue"
    items = col_dict[c]
    for item in items:
        #source = item.get_t_predicate()
        dest = item.get_t_object()
        if dest == 'refArea':
            return 'REF_AREA'
        if dest == 'classSystem':
            return 'CLASS_SYSTEM'
        if dest == 'refPeriod':
            return 'REF_PERIOD'
        return dest

    return None


def sdmx_report(sql, data_frame=None):
    """
    Build the sdmx file.

    :param sql: Sql text query.
    :param data_frame: The Pandas dataframe
    :return: Sdmx.
    """
    col_dict = build_column_dict(data_frame, sql)

    out_stream = StringIO.StringIO()
    generic_data = sdmx.GenericDataType()
    header = sdmx.HeaderType()
    header.set_Test(False)
    header.set_Truncated(False)
    ts = time.time()
    prepared = datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
    header.set_Prepared(prepared)
    sender = sdmx.PartyType()
    sender.set_id(SENDER)
    name = sdmx.TextType()
    name.set_lang(LANGUAGE_CODE)
    name.set_valueOf_(SENDER_NAME)
    sender.add_Name(name)
    header.add_Sender(sender)
    generic_data.set_Header(header)
    data_set = sdmx.DataSetType()
    data_set.set_keyFamilyURI(SENDER_NAME)
    data = get_data_from_data_frame(data_frame)
    for r, row in enumerate(data):
        series = sdmx.SeriesType()
        series_key = sdmx.SeriesKeyType()
        obs = sdmx.ObsType()
        for c in col_dict:
            v = row[c]
            val = "%s" % v
            value = sdmx.ValueType()
            concept = get_concept(col_dict, c)
            if concept == 'obsValue':
                if not val.startswith('*'):
                    obs_value = sdmx.ObsValueType()
                    obs_value.set_value(val)
                    obs.set_ObsValue(obs_value)
            elif concept is not None:
                value.set_concept(concept)
                value.set_value(val.strip())
                series_key.add_Value(value)
        series.set_SeriesKey(series_key)
        series.add_Obs(obs)
        data_set.add_Series(series)

    generic_data.set_DataSet(data_set)
    generic_data.export(out_stream, 0)

    value = out_stream.getvalue()
    value = add_xml_header(value)

    return value