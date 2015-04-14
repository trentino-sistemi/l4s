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

"""
Utils for l4s project.
"""

from django.db import connections, connection
from django.core.mail import send_mail
from django.contrib.sites.models import Site
from django.utils.translation import ugettext as _
from web.models import Metadata
from web.models import User
from web.exceptions import MissingMetadataException
from explorer.utils import url_get_params
from explorer.models import Query
from collections import OrderedDict
from l4s.settings import EXPLORER_DEFAULT_ROWS,\
    EXPLORER_DEFAULT_COLS,\
    DESCRIPTION_SUBJECT, DEFAULT_FROM_EMAIL, \
    EXPLORER_CONNECTION_NAME
import random
import re
import pandas as pd
import ast
import six
from datetime import datetime

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


METADATA = 'web_metadata'
USER_TYPE = 'web_usertype'
QUERY = 'explorer_query'
USER = 'web_user'
LOG = 'web_executedquerylog'
MANUAL_REQUEST = 'web_manualrequest'
LOCATED_IN_AREA = "http://dbpedia.org/ontology/locatedInArea"
CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
DIMENSION = 'http://purl.org/linked-data/cube#dimension'
MEASURE = 'http://purl.org/linked-data/cube#measure'
OBS_VALUE = 'http://purl.org/linked-data/sdmx/2009/measure#obsValue'
REF_PERIOD = 'http://purl.org/linked-data/sdmx/2009/dimension#refPeriod'
REF_AREA = 'http://purl.org/linked-data/sdmx/2009/dimension#refArea'
SAME_AS = 'http://www.w3.org/2002/07/owl#sameAs'
DESCRIPTION = 'http://purl.org/dc/terms/description'
SUBJECT = 'http://purl.org/linked-data/sdmx/2009/subject'
CONCEPT = 'http://purl.org/linked-data/cube#concept'
SQL_PREFIX = "sql://"

DESCRIPTION_TOKEN = "--INCLUDE_DESCRIPTIONS"
JOIN_TOKEN = '--JOIN'
AGGREGATION_TOKEN = '--AGGREGATION'
PIVOT_TOKEN = '--PIVOT'
DECLARE_TOKEN = '--DECLARE'
SET_TOKEN = '--SET'
WIDGET_TOKEN = '--WIDGET'
TOKENS = [DESCRIPTION_TOKEN, JOIN_TOKEN, AGGREGATION_TOKEN, PIVOT_TOKEN]
CODE = unicode(_("Code")).encode("utf-8")
TOTAL = unicode(_("Total")).encode("utf-8")
ALL = unicode(_("All")).encode("utf-8")


def get_class_range(val):

    query = "select class_from, class_to \n"
    query += "from web_classrange \n"
    query += "where (%s between class_from and class_to) or \n" % val
    query += "  (class_from is null and class_to >= %s) or \n" % val
    query += "  (class_to is null and class_from <= %s) \n"  % val

    rows = execute_query_on_django_db(query)

    for row in rows:
        class_from = row[0]
        class_to = row[1]

    if class_from == None:
        return  "<" + str(class_to)
    else:
        if class_to == None:
            return ">" + str(class_from)
        else:
            return str(class_from) + "-" + str(class_to)


def get_table_by_name_or_desc(search, tables, order):
    """
    Get tables by matching with search criteria.

    :param search: Expression to be search.
    :param order: Order table name by specified field.
    :return: Matching table list.
    """

    search_s = "'" + '%' + search + '%' + "'"
    query = "SELECT DISTINCT b.table_name, d.value \n"
    query += "FROM web_metadata b \n"
    query += "JOIN web_metadata d \n"
    query += "ON (d.table_name = b.table_name and d.column_name = 'NULL' "
    query += "and upper(d.key) = 'HTTP://PURL.ORG/DC/TERMS/DESCRIPTION') \n"
    query += "WHERE (b.column_name ilike %s or " % search_s
    query += "b.table_name ilike %s or " % search_s
    query += "d.value ilike %s) \n" % search_s
    if not tables is None:
        table_names = "'" + "','".join(tables) + "'"
        query += "and b.table_name IN (%s)" % table_names
    if not order is None:
        query += "ORDER BY %s" % order

    rows = execute_query_on_django_db(query)
    ret = OrderedDict()

    for row in rows:
        table_name = row[0]
        desc = row[1]
        ret[table_name] = desc

    return ret


def order_tables_by_descriptions(tables):
    """
    Order tables by descriptions.

    :param tables: List of table names.
    :return: A dictionary with descriptions.
    """
    ret_tables = []
    if tables is None or len(tables) == 0:
        return ret_tables

    tables_str = "'" + "','".join(tables) + "'"
    query = "SELECT table_name, value from %s \n" % METADATA
    query += "WHERE column_name ='NULL' \n"
    query += "and key='%s' \n" % DESCRIPTION
    query += "and table_name IN (%s) \n" % tables_str
    query += "ORDER BY value;"

    rows = execute_query_on_django_db(query)
    if rows is not None:
        for row in rows:
            table = row[0]
            ret_tables.append(table)
    return ret_tables


def order_tables_by_topic_and_descriptions(tables):
    """
    Order tables by topic and descriptions.

    :param tables: List of table names.
    :return: A dictionary with descriptions.
    """
    if tables is None or len(tables) < 2:
        return tables

    ret_tables = []
    tables_str = "'" + "','".join(tables) + "'"
    query = "SELECT b.argomento, a.nome from tabelle a \n"
    query += "JOIN argomenti_tabelle b \n"
    query += "ON (b.id = a.id)"
    query += "and a.nome IN (%s) \n" % tables_str
    query += "ORDER BY b.argomento"

    old_arg = None
    rows = execute_query_on_main_db(query)
    arg_tables = []
    for r, row in enumerate(rows):
        curr_arg = row[0]
        table_name = row[1]
        if not old_arg is None and (
                old_arg != curr_arg or r == len(rows) - 1):
            # order set and append it
            ord_tables = order_tables_by_descriptions(arg_tables)
            ret_tables.extend(ord_tables)
            arg_tables = []
        arg_tables.append(table_name)
        old_arg = curr_arg

    return ret_tables


def order_queries_by_topics(queries):
    """
    Order queries by topics (with trismegisto).

    :param queries:
    :return:
    """
    ordered_queries = []
    tab_top = sorted(queries, key=lambda x: x[1])
    query_pks = [z[0] for z in tab_top]
    for pk in query_pks:
        query = Query.objects.get(pk=pk)
        ordered_queries.append(query)
    return ordered_queries


def execute_query_on_main_db(query):
    """
    Execute a query on the main database.

    :param query: Sql query.
    :return: Rows, sql result set.
    """
    explorer_connection = connections[EXPLORER_CONNECTION_NAME]
    cursor = explorer_connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


def list_districts():
    """
    List district.

    :return: The list of the districts.
    """
    districs = []
    query = "SELECT descriz from trc1cotn WHERE comv!=0"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            districs.append("%s" % row[0])
    return districs


def list_valley_communities():
    """
    List the valley communities.

    :return: The list of valley communities.
    """
    valley_communities = []
    query = "SELECT descriz from trc4comv where comv != 0"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            valley_communities.append("%s" % row[0])
    return valley_communities


def list_tourism_sectors():
    """
    List the tourism sectors.

    :return: The list of the tourist sectors.
    """
    valley_communities = []
    query = "SELECT descriz from trcambtn"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            valley_communities.append("%s" % row[0])
    return valley_communities


def list_health_districts():
    """
    List the health_districts.

    :return: The list of the health districts.
    """
    valley_communities = []
    query = "SELECT descriz from sacdissa10"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            valley_communities.append("%s" % row[0])
    return valley_communities


def count_distinct(table_name, column_name):
    """
    Return the number of distinct values fro the table and column in input.

    :param table_name: Table name.
    :param column_name: Column name.
    :return:
    """
    query = "SELECT COUNT(DISTINCT %s) FROM %s" % (column_name, table_name)
    rows = execute_query_on_main_db(query)
    first_row = rows[0]
    return first_row[0]


def max_distinct_values_column(table_name,
                               table_description,
                               exclude):
    """
    Return the index with column with the maximum number of distinct values
    neglecting exclude indices.

    :param table_name: Table name.
    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    ret = -1
    max_occurrences = 0
    for c, field in enumerate(table_description):
        if c in exclude:
            continue
        else:
            column_name = field.name
            number_of_el = count_distinct(table_name, column_name)
            if c == 0 or number_of_el > max_occurrences:
                max_occurrences = number_of_el
                ret = c, column_name
    return ret, None


def min_distinct_values_column(table_name,
                               table_description,
                               exclude):
    """
    Return the index with column with the minimum number of distinct values
    neglecting exclude indices.

    :param table_name: Table name.
    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    ret = -1
    max_occurrences = 0
    for c, field in enumerate(table_description):
        if c in exclude:
            continue
        column_name = field.name
        if column_name is None:
            continue
        number_of_el = count_distinct(table_name, column_name)
        if c == 0 or number_of_el < max_occurrences:
            max_occurrences = number_of_el
            ret = c, column_name
    return ret, None


def first_ref_period_column(table_name,
                            table_description,
                            exclude):
    """
    Found first ref period column neglecting exclude indices.

    :param table_name: Table name.
    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    for f, field in enumerate(table_description):
        if f in exclude:
            continue
        column_name = field.name
        if is_ref_period(table_name, column_name):
            return f, column_name
    return -1, None


def list_ref_period(table_name,
                    table_description):
    """
    Get list of ref_period columns in table.

    :param table_name: Table name.
    :param table_description: Table schema description.
    :return: Columns containing a ref period.
    """
    columns = dict()
    query = "SELECT column_name FROM web_metadata \n"
    query += "WHERE table_name='%s' \n" % table_name
    query += "and value='%s'" % REF_PERIOD
    rows = execute_query_on_django_db(query)
    obs_set = []
    if not rows is None:
        for row in rows:
            obs_set.append(row[0])

    for f, field in enumerate(table_description):
        column_name = field.name
        if column_name in obs_set:
            columns[f] = column_name

    return columns


def contains_ref_period(pivot, cols, axis=None):
    """
    Get the pivot list and the columns structure and the axis to be checked.
    It returns if the table defined contains ref period values.

    :param pivot: Pivot cols.
    :param cols: Columns.
    :param axis: Axis where looking for.
    :return: Boolean.
    """
    if axis == 0:
        for c in pivot:
            table_name = cols[c]["table"]
            column_name = cols[c]["column"]
            if is_ref_period(table_name, column_name):
                return True
    if axis == 1:
        for c in cols:
            if not c in pivot:
                table_name = cols[c]["table"]
                column_name = cols[c]["column"]
                if is_ref_period(table_name, column_name):
                    return True
    return False


def all_hidden_fields(table_name,
                      table_description):
    """
    Get dict of hidden fields in table.

    :param table_name: Table name.
    :param table_description: Table descriptiopn.
    :return: All the field to be hidden.
    """
    query = "SELECT column_name FROM web_metadata \n"
    query += "WHERE table_name='%s' and \n" % table_name
    query += "upper(key)='VISIBLE' and upper(value)='FALSE'"
    rows = execute_query_on_django_db(query)
    res = []
    if rows is not None:
        for row in rows:
            res.append(row[0])

    ret = dict()
    for f, field in enumerate(table_description):
        column_name = field.name
        if column_name in res:
            ret[f] = column_name
    return ret


def first_column(table_description, exclude):
    """
    Found first column neglecting exclude indices.

    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    for f, field in enumerate(table_description):
        if f in exclude:
            continue
        column_name = field.name
        if column_name is None:
            continue
        return f, column_name
    return -1, None


def first_ref_area_column(table_name,
                          table_description,
                          exclude):
    """
    Found first ref area column neglecting exclude indices.

    :param table_name: Table name.
    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    for f, field in enumerate(table_description):
        if f in exclude:
            continue
        column_name = field.name
        if is_ref_area(table_name, column_name):
            return f, column_name
    return -1, None


def first_secret_column(table_name,
                        table_description,
                        exclude):
    """
    Found first ref area column neglecting exclude indices.

    :param table_name: Table name.
    :param table_description: Table description structure.
    :param exclude: Column index to be neglected.
    :return: index_column, column_name
    """
    for f, field in enumerate(table_description):
        if f in exclude:
            continue
        column_name = field.name
        if is_secret(table_name, column_name):
            return f, column_name
    return -1, None


def list_obs_value_column_from_dict(col_dict):
    """
    Found all observable value from dictionary
    and return indices.

    :param col_dict: Columns dictionary.
    :return: all observable values indices.
    """
    obs_list = []
    for v in col_dict:
        column_name = col_dict[v]['column']
        table_name = col_dict[v]['table']

        if is_obs_value(table_name, column_name):
            obs_list.append(v)

    return obs_list


def all_obs_value_column(table_name, table_description):
    """
    Found all observable value.

    :param table_name: Table name.
    :param table_description: Table description.
    :return: index_column, column_name
    """
    ret = OrderedDict()
    query = "SELECT column_name FROM web_metadata \n"
    query += "WHERE table_name='%s' \n" % table_name
    query += "and key='%s'" % MEASURE
    rows = execute_query_on_django_db(query)
    obs_set = []
    if not rows is None:
        obs_set = [row[0] for row in rows]

    for f, field in enumerate(table_description):
        column_name = field.name
        if column_name in obs_set:
            ret[f] = column_name

    return ret


def get_all_field_values(table_name, column_name, select):
    """
    Get all the value that can assume the field.

    :param table_name: Table name.
    :param column_name: Column name.
    :param select: Function to select if None than use column_name.
    :return: All values that can have each one column.
    """
    ret = []
    query = "--JOIN %s.%s 0\n" % (table_name, column_name)
    if select is None:
        query += "SELECT DISTINCT \"%s\" FROM %s\n" % (column_name, table_name) #quotato perche con postrgress se ci sono lettere maiuscole da errore
        query += "ORDER BY \"%s\"" % column_name
        st = detect_special_columns(query)

        #print bcolors.WARNING
        #print query

        query, h = build_description_query(query, st.cols, [], True, True)

        #print bcolors.FAIL
        #print query

        #print bcolors.ENDC

    else:
        query += "SELECT DISTINCT \"%s\" FROM %s\n" % (select, table_name)
        query += "ORDER BY \"%s\"" % select

    #print "query " , query

    rows = execute_query_on_main_db(query)
    for row in rows:
        if len(row) == 1:
            # Description is the code.
            new_row = [row[0], row[0]]
            ret.append(new_row)
        else:
            ret.append(row)

    #print "ret " , ret

    return ret


def choose_default_axis(table_name, ref_periods, hidden_fields):
    """
    Choose default axis to be selected in a table if the user don't specify
    parameters.

    :param table_name: The table name.
    :return: cols and rows chosen.
    """
    table_schema = get_table_schema(table_name)
    neglected_index = []
    cols = []
    rows = []

    for hidden_field in hidden_fields:
        neglected_index.append(hidden_field)

    obs_values = all_obs_value_column(table_name, table_schema)
    if len(obs_values) < 1:
        raise MissingMetadataException(MEASURE, OBS_VALUE, table_name)

    for index in obs_values:
        neglected_index.append(index)

    column_name = None
    for index in ref_periods:
        col_name = ref_periods[index]
        if col_name not in hidden_fields.values():
            column_name = col_name
            neglected_index.append(index)
            cols.append(column_name)

    if column_name is None:
        index, column_name = min_distinct_values_column(table_name,
                                                        table_schema,
                                                        neglected_index)
        if not column_name is None:
            neglected_index.append(index)
            cols.append(column_name)

    if column_name is None:
        index, column_name = first_column(table_schema,
                                          neglected_index)
        if not column_name is None:
            neglected_index.append(index)
            cols.append(column_name)

    index, column_name = first_secret_column(table_name,
                                             table_schema,
                                             neglected_index)
    if column_name is None:
        index, column_name = first_ref_area_column(table_name,
                                                   table_schema,
                                                   neglected_index)
    if column_name is None:
        index, column_name = max_distinct_values_column(table_name,
                                                        table_schema,
                                                        neglected_index)
    if column_name is None:
        index, column_name = first_column(table_schema,
                                          neglected_index)
    if not column_name is None:
        rows.append(column_name)

    if len(cols) == 0 or len(rows) == 0:
        error = _("I can not pivot the table")
        error = "%s '%s'" % (unicode(error), table_name)
        raise Exception(error)

    return cols, rows


def build_foreign_keys(table_name):
    """
    Get an hash table with foreign keys for the table specified.

    :param table_name: Table name.
    :return: An hash table with key the table name and value [table,column]
    """
    ret = dict()
    query = "select\n"
    query += "(\n"
    query += "select a.attname\n"
    query += "from pg_attribute a\n"
    query += "where a.attrelid = m.oid\n"
    query += "and a.attnum = o.conkey[1]\n"
    query += "and a.attisdropped = false\n"
    query += ") as src_colname,\n"
    query += "f.relname as dest_table,\n"
    query += "(\n"
    query += "select a.attname\n"
    query += "from pg_attribute a\n"
    query += "where a.attrelid = f.oid\n"
    query += "and a.attnum = o.confkey[1]\n"
    query += "and a.attisdropped = false\n"
    query += ") as dest_colname\n"
    query += "from pg_constraint o\n"
    query += "left join pg_class c on c.oid = o.conrelid\n"
    query += "left join pg_class f on f.oid = o.confrelid\n"
    query += "left join pg_class m on m.oid = o.conrelid\n"
    query += "where o.contype = 'f'\n"
    query += "and o.conrelid in (\n"
    query += "select oid from pg_class c where c.relkind = 'r'\n"
    query += ")\n"
    query += "and m.relname = '%s'\n" % table_name
    rows = execute_query_on_main_db(query)
    for row in rows:
        col_name = row[0]
        dest_table = row[1]
        dest_col = row[2]
        ret[col_name] = [dest_table, dest_col]

    return ret


def found_column_position(x, table_schema):
    """
    Return the position of x in table_schema

    :param x:
    :param table_schema:
    :return:
    """
    for f, field in enumerate(table_schema):
        if field.name == x:
            return f
    return -11


def build_query(table_name,
                columns,
                rows,
                obs_values,
                aggregation_ids,
                filters,
                values,
                not_agg_selection_value):
    """
    Build a query on table that select obsValues and then the desired
    cols and rows.
    REGARDS: The query is flat this return the query and the column list to
             used in pivot

    :param table_name: The table name.
    :param columns: Columns involved in the query
    :param rows: Rows involved in the query
    :param obs_values: Obs values.
    :param aggregation_ids: Ids of aggregations
    :param filters: filters to be applied
    :param values: values of contents
    :return: Query, column list to be used in pivot.
    """
    query = ""
    annotation = "%s\n" % DESCRIPTION_TOKEN
    fields = []
    obs_fields = []

    for agg in aggregation_ids:
        annotation += "%s %d\n" % (AGGREGATION_TOKEN, agg)

    position = 0
    for obs_value in obs_values:
        sum_s = "SUM(\"%s\") %s" % (obs_value, obs_value)
        obs_fields.append(sum_s)
        annotation += "%s " % JOIN_TOKEN
        annotation += "%s.%s %d\n" % (table_name, obs_value, position)
        position += 1

    col_positions = []
    for col in columns:
        if not col is None:
            annotation += "%s " % JOIN_TOKEN
            annotation += "%s.%s %d\n" % (table_name, col, position)
            annotation += "%s %d\n" % (PIVOT_TOKEN, position)
            col_positions.append(position)
            fields.append(" \"%s\" " % col)
            position += 1

    for row in rows:
        if not row is None:
            annotation += "%s " % JOIN_TOKEN
            annotation += "%s.%s %d\n" % (table_name, row, position)
            fields.append(" \"%s\" " % row)
            position += 1

    comma_sep_fields = ", ".join(fields)
    query += "\n"
    query += 'SELECT '
    if len(obs_fields) > 0:
        comma_sep_threshold_fields = ", ".join(obs_fields)
        query += "%s, " % comma_sep_threshold_fields
    query += '%s' % comma_sep_fields
    query += '\nFROM %s' % table_name

    #print query

    for id in not_agg_selection_value:

        #print "id ", id

        query_filtri_aggregazione = "SELECT table_name, column_name, value FROM %s \n" % METADATA
        query_filtri_aggregazione += "WHERE id IN(%s) \n" % id

        #print "query_filtri_aggregazione " , query_filtri_aggregazione

        rows = execute_query_on_django_db(query_filtri_aggregazione)
        first_row = rows[0]

        table_name_join = first_row[0]
        column_name_join = first_row[1]
        value_join = first_row[2]

        ref_table, ref_column = located_in_area(table_name_join,
                                                column_name_join,
                                                value_join)

        #print "ref_table ", ref_table
        #print "ref_column " , ref_column

        foreign_keys = build_foreign_keys(table_name)

        for id_fk, column in enumerate(foreign_keys):

            elements = foreign_keys[column]

            #print elements

            if elements[0] == table_name_join and elements[1] == column_name_join:
                orig_column = column

        field_name_join = '%s."%s" = %s."%s"' % (ref_table, column_name_join, table_name, orig_column)

        query += '\njoin %s on (%s) ' % (ref_table, field_name_join)

        #print id

        #print not_agg_selection_value[id]

    filtered = False

    #print "filters ", filters

    for field in filters:
        filter_vals = filters[field]

        if len(values[field]) != len(filter_vals):
            selected_vals = [str(val[0]) for val in filter_vals]
            if filter_vals is not None and len(filter_vals) > 0:
                if filtered:
                    query += '\nAND'
                else:
                    query += '\nWHERE'
                query += " \"%s\" IN (" % field
                comma_sep_vals = ", ".join(selected_vals)
                query += "%s )" % comma_sep_vals
                filtered = True

    for id in not_agg_selection_value:

        query_filtri_aggregazione = "SELECT table_name, column_name, value FROM %s \n" % METADATA
        query_filtri_aggregazione += "WHERE id IN(%s) \n" % id

        #print "query_filtri_aggregazione " , query_filtri_aggregazione

        rows = execute_query_on_django_db(query_filtri_aggregazione)
        first_row = rows[0]

        table_name_join = first_row[0]
        column_name_join = first_row[1]
        value_join = first_row[2]

        ref_table, ref_column = located_in_area(table_name_join,
                                                column_name_join,
                                                value_join)

        if filtered:
            query += '\nAND'
        else:
            query += '\nWHERE'

        ag_vals = []

        for id_value, values in enumerate(not_agg_selection_value[id]):
            #print values[0]
            ag_vals.append(str(values[0]))

        if len(ag_vals) > 0:
            #print "ag_vals ", ag_vals
            comma_sep_ag_vals = ", ".join(ag_vals)

            #print comma_sep_ag_vals

            query += '  %s."%s" ' % (ref_table, ref_column)
            query += "IN (%s)" % comma_sep_ag_vals

        filtered = True

        #print not_agg_selection_value[id]

    if len(obs_fields) > 0:
        query += "\nGROUP BY %s" % comma_sep_fields

    query = "%s\n%s\n" % (annotation, query)

    #print "query " , query

    return query, col_positions


def extract_header(sql):
    """
    Separate our directives (header) by sql body.

    :param sql: Sql text.
    :return: header, sql (without directives)
    """
    clean_sql = ""
    header = ""
    for line in sql.strip().splitlines():
        left_stripped_line = line.lstrip(' ')
        words = left_stripped_line.split(' ')
        declare = words[0]
        if declare in TOKENS:
            header += "%s\n" % line
        else:
            clean_sql += "%s\n" % line
    return header, clean_sql


def find_table_description_column(table_name):
    """
    Get the column name with description.

    :return: the column name with description.
    """
    query = "SELECT column_name\n"
    query += "FROM %s\n" % METADATA
    query += "WHERE column_name  != 'NULL'"
    query += "and table_name='%s' " % table_name
    query += "and key = '%s'" % SAME_AS
    query += "and value='%s'" % DESCRIPTION_SUBJECT

    #print "find_table_description_column ", query

    rows = execute_query_on_django_db(query)
    if not rows is None:
        for row in rows:
            return row[0]
    raise MissingMetadataException(SAME_AS, DESCRIPTION, table_name)


def remove_code_from_data_frame(df):
    """
    Remove code from data frame.

    :param df: Data frame.
    """
    #print df.index.get_loc((4,'Alta Valsugana e Bersntol                         '))

    # Rename empty index with total index name.
    df.rename(index={u"": TOTAL}, inplace=True)
    # Rename empty columns with total column name.
    df.rename(columns={u"": TOTAL}, inplace=True)

    #print df.index.get_loc((4,'Alta Valsugana e Bersntol                         '))

    dropped_levels = 0
    # Now I can drop the codes indices preserving totals.
    for level, column in enumerate(df.columns.names):
        if column is not None and column.startswith(CODE):
            df.columns = df.columns.droplevel(level-dropped_levels)
            dropped_levels += 1

    #print "ddff " , df.index.get_loc((4,'Alta Valsugana e Bersntol                         '))

    #print df.index.levels

    #print "df.index.lexsort_depth " , df.index.lexsort_depth
    #print df.index.values.tolist()

    dropped_levels = 0
    # Now I can drop the codes columns preserving totals.
    for level, index in enumerate(df.index.names):

        #print level, index

        if index is not None and index.startswith(CODE):

            #print index

            desc = index[len(CODE)+1:]

            #print desc

            if desc in df.index.names:

                df.index = df.index.droplevel(level-dropped_levels)
                dropped_levels += 1

                #if has_data_frame_multi_level_index(df):  #riordina il data_freame per le righe...
                #    df = df.sortlevel(axis=0)
                #else:
                #    df = df.sort_index(axis=0)

                #print df.index.levels

                #print "df.index.lexsort_depth " , df.index.lexsort_depth

                #print df.index.values.tolist()

                #print df.index.get_loc(('Alta Valsugana e Bersntol                         ',))





    return df


def is_to_be_sorted_by_description(foreign_keys, column):
    """
    Is the column to be sorted by description?

    :param foreign_keys: Foreign keys.
    :param column: Column name.
    :return: Boolean.
    """
    f = foreign_keys[column]
    f_table = f[0]
    f_column = f[1]
    val = get_key_column_value(f_table, f_column, "order_by")
    if val is not None and val == "description":
        return False
    return True


def build_description_query(query, fields, pivot_cols, order, include_code):
    """
    Take a query with the table fields structure and return a query enriched
    by descriptions.
    REGARDS: The code are included for columns to be sorted by code also.
             The code must be removed if not requested by the user
             with the function remove_code_from_data_frame.

    :param query: query to be enriched.
    :param fields: fields structure.
    :param pivot_cols: pivot cols.
    :param order: order by desc.
    :param include_code: Force to include code.
    :return: query with descriptions.
    """

    query_header = []
    header, inner_sql = extract_header(query)
    new_sql_header = ""
    main_table = "main_table"
    desc_query = "SELECT "
    fk_hash = dict()
    counter = 0

    #print fields

    for f in fields:
        #print f
        if f == -1:
            continue
        table = fields[f]['table']
        field = fields[f]['column']
        if f != 0:
            desc_query += ", \n"
        if field is None:
            desc_query += "%s" % table
            continue
        if not table in fk_hash:
            foreign_keys = build_foreign_keys(table)
            fk_hash[table] = foreign_keys
        else:
            foreign_keys = fk_hash[table]

        #print "foreign_keys ", foreign_keys

        if field in foreign_keys:
            fk = foreign_keys[field]
            dest_table = fk[0]

            if is_decoder_table(dest_table):

                dest_column = fk[1]
                desc_column = find_table_description_column(dest_table)
                alias = get_column_description(dest_table, desc_column)

                """
                print "dest_table " + dest_table
                print "dest_column " + dest_column
                print "desc_column " + desc_column
                print "alias " + alias
                """

                if alias is None:
                    alias = dest_column
                alias = "%s" % alias
                alias = alias.strip() #toglie gli spazi all'inizio e alla fine
                sort_by_code = is_to_be_sorted_by_description(fk_hash[table],
                                                              field)

                #print sort_by_code

                if include_code or sort_by_code:
                #if True:
                    desc_query += "%s.%s " % (main_table, field)
                    code_name = "%s %s" % (CODE, alias)
                    desc_query += "AS \"%s\", " % code_name
                    query_header.append(code_name)
                    new_sql_header += "%s %s.%s %d \n" % (JOIN_TOKEN,
                                                          dest_table,
                                                          desc_column,
                                                          counter)

                    if f in pivot_cols:
                        new_sql_header += "%s %d \n" % (PIVOT_TOKEN, counter)
                    counter += 1

                new_sql_header += "%s %s.%s %d \n" % (JOIN_TOKEN,
                                                      table,
                                                      field,
                                                      counter)

                if f in pivot_cols:
                    new_sql_header += "%s %d \n" % (PIVOT_TOKEN, counter)
                counter += 1
                j_table = "%s_%s" % (dest_table, field)
                desc_query += "%s.%s " % (j_table, desc_column)
                desc_query += "AS \"%s\"" % alias
                query_header.append(alias)
                continue

        alias = get_column_description(table, field)
        if alias is None:
            alias = field
        alias = "%s" % alias
        alias = alias.strip()
        desc_query += "%s.\"%s\" AS \"%s\"" % (main_table, field, alias)
        query_header.append(alias)
        new_sql_header += "%s " % JOIN_TOKEN
        new_sql_header += "%s.%s %d \n" % (table, field, counter)
        if f in pivot_cols:
            new_sql_header += "%s %d \n" % (PIVOT_TOKEN, counter)
        counter += 1

    desc_query += "\nFROM\n"
    desc_query += "(\n%s\n) AS %s" % (inner_sql, main_table)

    for f in fields:
        field = fields[f]['column']
        if field is None:
            continue
        table = fields[f]['table']
        foreign_keys = fk_hash[table]

        if field in foreign_keys:
            fk = foreign_keys[field]
            dest_table = fk[0]
            if is_decoder_table(dest_table):
                dest_column = fk[1]
                alias = "%s_%s" % (dest_table, field)
                desc_query += "\nJOIN %s %s ON " % (dest_table, alias)
                desc_query += "(%s.%s=" % (main_table, field)
                desc_query += "%s.%s)" % (alias, dest_column)

    desc_query = "%s\n%s\n" % (new_sql_header, desc_query)

    if order:
        for f in fields:
            field = fields[f]['column']
            if field is None:
                continue
            table = fields[f]['table']
            foreign_keys = fk_hash[table]
            if field in foreign_keys:
                fk = foreign_keys[field]
                dest_table = fk[0]
                if is_decoder_table(dest_table):
                    desc_column = find_table_description_column(dest_table)
                    if order:
                        alias_t = "%s_%s" % (dest_table, field)
                        desc_query += "ORDER BY %s." % alias_t
                        desc_query += "%s" % desc_column

    #print "desc_query " , desc_query

    return desc_query, query_header


def saved_data_years():
    """
    Get the years in descendant order of saved data
    (queries and manual requests)

    :return: rows
    """
    query = "SELECT DISTINCT "
    query += "cast(extract('year' from request_date) as int) y \n"
    query += "FROM %s \n" % MANUAL_REQUEST
    query += "UNION \n"
    query += "SELECT DISTINCT "
    query += "cast(extract('year' from created_at) as int) y \n"
    query += "FROM %s \n" % QUERY
    query += "ORDER BY y DESC"
    rows = execute_query_on_django_db(query)
    return [row[0] for row in rows]


def saved_queries_grouped_by_user_type(year, month):
    """
    Return the number of saved queries grouped by user type.

    :return: list of tuples <user_type, occurrences>
    """

    query = "SELECT UT.name, count(UT.name) \n"
    query += "FROM %s EQ join %s U \n" % (QUERY, USER)
    query += "ON (U.email = EQ.created_by) \n"
    query += "JOIN %s UT ON (UT.id = U.user_type_id) \n" % USER_TYPE
    query += "WHERE extract('year' from EQ.created_at) = '%d' \n" % year
    if not month is None:
        query += "AND extract('month' from EQ.created_at) = '%d' \n" % month
    query += "GROUP BY UT.name \n"
    query += "ORDER BY count(UT.name) DESC "

    rows = execute_query_on_django_db(query)
    return rows


def run_queries_auth(year, month):
    """
    Get the number of executed queries from authenticated users.

    :param year: Year
    :param month: Month
    :return: rows
    """
    query = "SELECT UT.name, count(UT.name) \n"
    query += "FROM %s EQ join %s U \n" % (LOG, USER)
    query += "ON (EQ.executed_by<>-1 AND U.id = EQ.executed_by) \n"
    query += "JOIN %s UT ON (UT.id = U.user_type_id) \n" % USER_TYPE
    query += "WHERE extract('year' from EQ.executed_at) = '%d' \n" % year
    if not month is None:
        query += "AND extract('month' from EQ.executed_at) = '%d' \n" % month
    query += "GROUP BY UT.name \n"
    query += "ORDER BY count(UT.name) DESC "
    rows = execute_query_on_django_db(query)
    return rows


def run_queries_anon(year, month):
    """
    Get the number of executed queries from authenticated users.

    :param year: Year
    :param month: Month
    :return: rows
    """
    query = "SELECT  count(*) \n"
    query += "FROM %s \n" % LOG
    query += "WHERE executed_by=-1 AND "
    query += "extract('year' from executed_at) = '%d' \n" % year
    if not month is None:
        query += "AND extract('month' from executed_at) = '%d' \n" % month
    rows = execute_query_on_django_db(query)
    return rows


def saved_manual_requests_grouped_by_user_type(year, month):
    """
    Return the number of manual requests grouped by user type.

    :param year: Year.
    :param month: Month.
    :return: list of tuples <user_type, occurrences>
    """
    query = "SELECT UT.name, count(UT.name) \n"
    query += "FROM %s MR join %s U \n" % (MANUAL_REQUEST, USER)
    query += "ON (U.id = MR.inquirer_id) \n"
    query += "JOIN %s UT ON (UT.id = U.user_type_id) \n" % USER_TYPE
    query += "WHERE extract('year' from MR.request_date) = '%d' \n" % year
    if not month is None:
        query += "AND extract('month' from MR.request_date) = '%d' \n" % month
    query += "GROUP BY UT.name \n"
    query += "ORDER BY count(UT.name) DESC "
    rows = execute_query_on_django_db(query)
    return rows

def build_constraint_query_test(constraints,
                           col_dict,
                           filters,
                           aggregations):
    """
    Build ad hoc query on related table.

    :param constraints: Constraints.
    :param col_dict: Column dictionary.
    :param filters: Filters.
    :return: The new query applying constraints.
    """

def build_constraint_query(constraints,
                           col_dict,
                           filters,
                           aggregations,
                           old_cols):
    """
    Build ad hoc query on related table.

    :param constraints: Constraints.
    :param col_dict: Column dictionary.
    :param filters: Filters.
    :return: The new query applying constraints.
    """

    #print filters
    #print "old_cols " , old_cols

    table = constraints[0]['table']
    enum_column = constraints[0]['column']

    dest_table_description = get_table_schema(table)

    dest_columns = [field.name for field in dest_table_description]

    columns = []
    for col in old_cols:

        col_name = old_cols[col]['column']

        if col_name in dest_columns:
            columns.append(col_name)

    """
    for col in col_dict:
        col_name = col_dict[col]['column']

        print col_name

        if col_name in dest_columns:
            columns.append(col_name)
    """

    if len(columns) == 0:
        return None, None

    header = []
    query = ""
    c = 0

    for c, column in enumerate(columns):  #mette il --join all'inizio della query
        query += "%s %s.%s %d \n" % (JOIN_TOKEN, table, column, c)
    query += "%s %s.%s %d \n\n" % (JOIN_TOKEN, table, enum_column, c+1)

    fields = ','.join([k for k in columns])  #mette il select dei campi che servono
    query += "SELECT %s, " % fields
    for col in columns:
        header.append(col)

    #for enum_column in enum_columns:
    query += "SUM(%s) %s \n" % (enum_column, enum_column)  #mette il sum del campo che detiene il numero degli alberghi

    header.append(enum_column)
    query += "FROM %s \n" % table  #aggiunge il from
    counter = 0

    """
    for i, k in enumerate(columns):  #questo non lo capisco
        src_table = col_dict[i]['table']
        if counter == 0:
            query += "WHERE "
        else:
            query += "AND "
        counter += 1
        query += "%s IN (SELECT DISTINCT %s from %s ) \n" % (k, k, src_table)
    """

    for f in filters:

        #if not f in columns and f in dest_columns:
        if f in dest_columns:
            filter_value = filters[f]
            if len(filter_value) > 0:
                values = ','.join(["%s" % k[0] for k in filter_value])
                if counter == 0:
                    query += "WHERE "
                else:
                    query += "AND "
                query += " %s IN (%s)\n" % (f, values)
            counter += 1

    query += "GROUP BY %s \n" % fields

    if len(aggregations) == 0:
        query += "HAVING "

        for c, constraint in enumerate(constraints):
            if c != 0:
                query += " AND "
            operator = constraint["operator"]
            value = constraint["value"]
            query += "SUM(%s)%s%s" % (enum_column, operator, value)

    query += "\nORDER BY %s" % fields

    return query, header

def item_constraint(constraint):
    """
    Take a constraint string and return a lazy structure
    representing the item.

    :param constraint: Constraint from metadata to be used to
                       perform primary suppression.
    :return: Item.
    """
    lesser_than = "<"
    lesser_than_equals = "<="
    greater_than = ">"
    greater_than_equals_token = ">="
    equals_than_token = "="
    words = constraint.split('.')
    table_name = words[0]
    if lesser_than in words[1]:
        words = words[1].split(lesser_than)
        operator = lesser_than
    elif greater_than in words[1]:
        words = words[1].split(greater_than)
        operator = greater_than
    elif equals_than_token in words[1]:
        words = words[1].split(equals_than_token)
        operator = equals_than_token
    elif lesser_than_equals in words[1]:
        words = words[1].split(lesser_than_equals)
        operator = lesser_than_equals
    elif greater_than_equals_token in words[1]:
        words = words[1].split(greater_than_equals_token)
        operator = greater_than_equals_token
    else:
        return None
    column_name = words[0]
    value = words[1]
    item = dict()
    item['table'] = table_name
    item['column'] = column_name
    item['operator'] = operator
    item['value'] = value
    return item


def build_constraint_dict(constraint_cols):
    """
    Build a dictionary with constraint.
    :param constraint_cols: Columns containing some constraints.
    """
    constraint_dict = dict()
    and_s = "AND"

    for c in constraint_cols:
        res = []
        value = constraint_cols[c]
        if and_s in value:
            constraints = value.split(and_s)
            for constraint in constraints:
                item = item_constraint(constraint)
                res.append(item)
        else:
            item = item_constraint(value)
            res.append(item)
        constraint_dict[c] = res

    return constraint_dict

def build_secondary_query(secondary,
                          col_dict,
                          filters,
                          old_cols,
                          aggregations):
    """
    Build ad hoc query on related table.

    :param secondary: Constraint to be followed in order to apply
                      the secondary suppression.
    :param col_dict: Column dictionary.
    :param filters: Filters.
    :return: The new query applying constraints.
    """
    #print "secondary " , secondary

    second_clause = secondary.split()[2]
    first_word = second_clause.split('>')[0]
    second_word = second_clause.split('>')[1]

    constraint_cols = dict()
    constraint_cols[0] = second_clause
    constraint_dict = build_constraint_dict(constraint_cols)
    constraint_values = constraint_dict[0]

    second_clause = 'SUM(' + first_word + ')>' + second_word

    tpc = first_word.split('.')
    table = tpc[0]
    enum_column = tpc[1]

    dest_table_description = get_table_schema(table)
    dest_columns = [field.name for field in dest_table_description]

    columns = []
    for col in old_cols:
        col_name = old_cols[col]['column']
        if col_name in dest_columns:
            columns.append(col_name)

    if len(columns) == 0:
        return None, None

    header = []
    query = ""
    c = 0
    for c, column in enumerate(columns):
        query += "%s %s.%s %d \n" % (JOIN_TOKEN, table, column, c)
    query += "%s %s.%s %d \n\n" % (JOIN_TOKEN, table, enum_column, c+1)

    fields = ','.join([k for k in columns])
    query += "SELECT %s, " % fields
    for col in columns:
        header.append(col)
    #for enum_column in enum_columns:
    query += "SUM(%s) %s \n" % (enum_column, enum_column)
    header.append(enum_column)
    query += "FROM %s \n" % table
    counter = 0

    """
    for i, k in enumerate(columns):
        src_table = col_dict[i]['table']
        if counter == 0:
            query += "WHERE "
        else:
            query += "AND "
        counter += 1
        query += "%s IN (SELECT DISTINCT %s from %s ) \n" % (k, k, src_table)
    """

    for f in filters:
        if f in dest_columns:
            filter_value = filters[f]
            if len(filter_value) > 0:
                if counter == 0:
                    query += "WHERE "
                else:
                    query += "AND "
                values = ','.join(["%s" % k[0] for k in filter_value])
                query += " %s IN (%s)\n" % (f, values)
                counter += 1

    query += "GROUP BY %s \n" % fields

    if len(aggregations) == 0:
        query += "HAVING %s\n" % second_clause

    query += "\nORDER BY %s \n" % enum_column
    query += "\nDESC "


    return constraint_values, table, enum_column, query, header


def add_secret_column(secret_cols, index, table_name, column_name):
    """
    Add secret info to hash table.

    :param secret_cols: Secret columns.
    :param index: Index.
    :param table_name: Table name.
    :param column_name: Column name.
    :return: the secret columns.
    """
    if is_secret(table_name, column_name):
        secret_cols[index] = dict()
        secret_cols[index]['table'] = table_name
        secret_cols[index]['column'] = column_name
    return secret_cols


def add_constraint_column(constraint_cols, index, table_name, column_name):
    """
    Add constraint info to hash table.

    :param constraint_cols: COnstraint columns.
    :param index: Index.
    :param table_name: Table name.
    :param column_name: Column name.
    :return: The constraint column.
    """
    constraint = get_constraint(table_name, column_name)

    if constraint is not None:
        constraint_cols[index] = dict()
        constraint_cols[index] = constraint
    return constraint_cols


def add_decoder_column(decoder_cols, index, table_name):
    """
    Add decoder info to hash table.

    :param decoder_cols: Decoder columns.
    :param index: Index.
    :param table_name: Table name.
    :return: Decoder columns.
    """
    if is_decoder_table(table_name):
        decoder_cols.append(index)
    return decoder_cols


def add_threshold_column(threshold_cols, index, table_name, column_name):
    """
    Add threshold info to hash table.

    :param threshold_cols: Threshold columns.
    :param index: Index.
    :param table_name: Table name.
    :param column_name: Column name.
    :return: Threshold columns.
    """
    threshold = get_threshold(table_name, column_name)
    if threshold is not None:
        threshold_cols[index] = float(threshold)
    return threshold_cols


def add_column(cols, index, table_name, column_name):
    """
    Add column info to hash table.

    :param cols: Columns.
    :param index: Index.
    :param table_name: Table name.
    :param column_name: Column name.
    :return: Columns
    """
    cols[index] = dict()
    cols[index]['table'] = table_name
    cols[index]['column'] = column_name
    return cols


def add_secret_ref(secret_ref, table_name, column_name):
    """
    Add secret info to hash table.

    :param secret_ref: Secret...
    :param table_name: Table name.
    :param column_name: Column name.
    :return: Secret...
    """
    sec = dict()
    sec['table'] = table_name
    sec['column'] = column_name
    secret_ref.append(sec)
    return secret_ref


def get_data_from_data_frame(df):
    """
    Extract a data matrix from a pandas data frame.

    :param df: Pandas data frame.
    :return: List of tuples representing the data in table.
    """
    return [x for x in df.to_records(index=False)]


def change_query(model_instance, pivot_cols, aggregation_ids):
    """
    Take the sql field of Query model form and remove all
    the --PIVOT statements replacing them with the right ones
    defined with the form and taken by the pivot_cols parameter.
    Do same thing with --AGGREGATION.

    :param model_instance: Query Model Form.
    :param pivot_cols: The set of pivot columns.
    """
    sql = ""
    for p in pivot_cols:
        sql += "%s %d\n" % (PIVOT_TOKEN, p)

    for a in aggregation_ids:
        sql += "%s %d\n" % (AGGREGATION_TOKEN, a)

    for line in model_instance.sql.splitlines():
        s_line = line.lstrip()
        if not s_line.startswith(PIVOT_TOKEN):
            if not s_line.startswith(AGGREGATION_TOKEN):
                sql += "%s\n" % line

    model_instance.sql = sql


def unpivot(df):
    """
    Take a pivot table and unpivot it preserving the column number.
    This is useful to have a plain table from a pivoted one.

    :param df: Pandas Data frame.
    :return: Un-pivoted Pandas data frame.
    """
    cols = []
    for c, col in enumerate(df.columns.names):
        if col is not None:
            cols.append(col)

    for i, index in enumerate(df.index.names):
        cols.append(index)

    obs_value = df.columns.levels[0][0]
    cols.append(obs_value)

    df = drop_total_column(df)
    df = drop_total_row(df)
    df = df.stack().reset_index()
    df = df.dropna()

    df = df.reindex_axis(cols, axis=1)

    return df


class Symboltable(object):
    """
    Class to be used as symbol table of the our meta-SQL language.
    """

    def __init__(self):
        self.include_descriptions = False
        self.cols = dict()
        self.threshold = dict()
        self.secret = dict()
        self.constraint = dict()
        self.secret_ref = []
        self.decoder = []
        self.pivot = []
        self.aggregation = []


def detect_special_columns(sql):
    """
    Return the list of the columns index sensitive to the statistical secret
    and the list of the columns to be protected with the statistical secret
    if are below a given threshold.

    :param sql: Sql query text.
    :return: The symbol table.
    """
    st = Symboltable()

    #print "sql ", sql

    for line in sql.splitlines():
        left_stripped_line = line.lstrip(' ')
        words = left_stripped_line.split(' ')
        first_word = words[0]
        if first_word == JOIN_TOKEN:
            table_and_column = words[1]
            table_name = table_and_column
            column_name = None
            if "." in table_and_column:
                tpc = table_and_column.split('.')
                table_name = tpc[0]
                column_name = tpc[1]
                if len(words) == 2:
                    add_secret_ref(st.secret_ref, table_name, column_name)
                else:
                    index = ast.literal_eval(words[2])
                    add_secret_column(st.secret,
                                      index,
                                      table_name,
                                      column_name)
                    add_constraint_column(st.constraint, index, table_name,
                                          column_name)
                    add_decoder_column(st.decoder, index, table_name)
                    add_threshold_column(st.threshold, index, table_name,
                                         column_name)
                    add_column(st.cols, index, table_name, column_name)
            else:
                index = ast.literal_eval(words[1])
                add_column(st.cols, index, table_name, column_name)
        elif first_word == PIVOT_TOKEN:
            position = ast.literal_eval(words[1])
            st.pivot.append(position)
        elif first_word == AGGREGATION_TOKEN:
            agg_id = ast.literal_eval(words[1])
            st.aggregation.append(agg_id)
        elif first_word == DESCRIPTION_TOKEN:
            st.include_descriptions = True
        else:
            continue

    return st


def build_aggregation_query(sql, cols, aggregations, agg_filters, threshold, constraints):
    """
    Build aggregation query.

    :param sql: Sql text.
    :param cols: Columns.
    :param aggregations: Aggregations.
    :param threshold: Threshold.
    :return: Sql (aggregated query).
    """

    #print sql
    #print "aggregations ", aggregations

    err = None
    cols_s = ','.join([str(k) for k in aggregations])

    #print "cols_s ", cols_s

    query = "SELECT table_name, column_name, count(id) FROM %s \n" % METADATA
    query += "WHERE id IN(%s) \n" % cols_s
    query += "GROUP BY table_name, column_name ORDER BY count(id) DESC "

    #print "query " , query

    rows = execute_query_on_django_db(query)
    first_row = rows[0]
    num = first_row[2]
    if num > 1:  # boh .. e questo quando si verifica ?
        table_name = first_row[0]
        column_name = first_row[1]
        desc = get_column_description(table_name, column_name)
        fl = _(
            "You can aggregate for a single criterion for each column only.")
        sl = _("You have selected more than one aggregation on column")
        err = unicode(fl) + " " + unicode(sl) + " "
        if desc is not None and desc != "":
            wd = _("with description")
            err += unicode(wd) + " \'" + desc + "\'."
        else:
            err += " " + column_name
        return sql, err

    #print bcolors.OKBLUE
    #print "query aggregazione prima ", sql

    for a, aggregation in enumerate(aggregations):
        metadata = Metadata.objects.get(id=aggregation)

        if a != 0:
            st = detect_special_columns(sql)
            cols = st.cols
            threshold = st.threshold

        metadata_value = "%s" % metadata.value
        if metadata_value.startswith(SQL_PREFIX):
            formula, alias = get_class_formula_alias(aggregation)
            sql = build_class_query(sql,
                                    cols,
                                    metadata.column_name,
                                    formula,
                                    None,
                                    threshold)
        else:
            sql = build_located_in_area_query(sql, cols, metadata, agg_filters,
                                              threshold, constraints)


    #print "metadata_value ", metadata_value
    #print "query aggregazione dopo ", sql
    #print bcolors.ENDC

    return sql, err


def build_located_in_area_query(sql, cols, metadata, agg_filters, threshold, constraints):
    """
    Build query for geo spatial aggregation.

    :param sql: The sql text.
    :return: The new query to aggregate to an higher level.
    """

    #print "threshold " , threshold
    #print "cols " , cols

    ref_table, ref_column = located_in_area(metadata.table_name,
                                            metadata.column_name,
                                            metadata.value)

    foreign_keys = build_foreign_keys(cols[0]['table'])

    for id, column in enumerate(foreign_keys):

        elements = foreign_keys[column]

        #print elements

        if elements[0] == metadata.table_name and elements[1] == metadata.column_name:
            orig_column = column

    """
    print "orig_column ", orig_column

    print "metadata.table_name " , metadata.table_name
    print "metadata.column_name " , metadata.column_name
    print "metadata.value " , metadata.value

    print "ref_table " , ref_table
    print "ref_column " , ref_column
    print "cols " , cols
    print "agg_filters " , agg_filters
    """

    query = "SELECT "
    params = ""
    new_table = metadata.table_name + "_" + ref_table
    old_header, inner_sql = extract_header(sql)

    header = ""
    if DESCRIPTION_TOKEN in old_header:
        header += DESCRIPTION_TOKEN + "\n"

    header += "%s " % JOIN_TOKEN
    header += "%s.%s\n" % (metadata.table_name, metadata.column_name)
    for c in cols:
        table = cols[c]['table']
        column = cols[c]['column']
        if c != 0:
            query += ", "
        if c in threshold:
            query += "SUM(%s.%s) %s " % (new_table, column, column)
            header += "%s " % JOIN_TOKEN
            header += "%s.%s %s\n" % (table, column, c)
            continue

        if column == orig_column:
            query += ref_table + "." + ref_column
            header += "%s " % JOIN_TOKEN
            header += "%s.%s" % (ref_table, ref_column)
            header += " %s\n" % c
            if params != "":
                params += ','
            params += "%s.%s " % (ref_table, ref_column)
            continue

        query += "%s.%s" % (new_table, column)
        if params != "":
            params += ', '
        params += "%s.%s" % (new_table, column)
        header += "%s " % JOIN_TOKEN
        header += "%s.%s " % (table, column)
        header += str(c) + "\n"

    query += "\nFROM (%s) %s JOIN %s" % (inner_sql, new_table, ref_table)
    query += "\nON (%s.%s=" % (new_table, orig_column)
    query += "%s.%s" % (ref_table, orig_column)

    pk = str(metadata.pk)

    #print "pk ", pk

    if pk in agg_filters:
        ag_vals = []
        agg = agg_filters[pk]
        for n, ag in enumerate(agg):
            val = "%s" % ag[0]
            ag_vals.append(val)
        if len(ag_vals) > 0:
            comma_sep_ag_vals = ", ".join(ag_vals)
            query += " AND %s.%s " % (ref_table, ref_column)
            query += "IN (%s)" % comma_sep_ag_vals

    query += ")"

    query += "\nGROUP BY %s" % params

    if len(constraints) != 0:
        query += " HAVING "

        for c, constraint in enumerate(constraints):
            if c != 0:
                query += " AND "
            operator = constraint["operator"]
            value = constraint["value"]
            enum_column = constraint['column']

            query += "SUM(%s)%s%s" % (enum_column, operator, value)


    query += "\nORDER BY %s" % params

    query = header + query

    #print "query ", query
    save_value('manuel', query)

    return query


def get_class_formula_alias(aggregation):
    """
    Get formula and alias from a class aggregation metadata.

    :param aggregation: Aggregation id.
    :return: Formula, alias.
    """
    metadata = Metadata.objects.get(id=aggregation)
    column_name = metadata.column_name
    value = metadata.value
    value = value[len(SQL_PREFIX):]
    url = re.findall(r'(https?://[^\s]+)', value)[0].strip(",")
    subs = value.replace(url, column_name)
    alias = re.findall(r'AS(.*)', value)[0]
    formula = subs.replace('AS' + alias, "")
    return formula, alias


def build_class_query(sql, cols, column_name, formula, alias, threshold):
    """
    Build query for CLASS aggregation.

    :param sql: The sql text.
    :param cols: Columns.
    :param column_name:
    :param formula:
    :param alias:
    :param threshold: Threshold.
    :return: The new query to aggregate to an higher CLASS level.
    """

    query = "SELECT "
    params = ""
    new_table = 'a'
    header = ""
    for c in cols:
        table = cols[c]['table']
        column = cols[c]['column']
        if c != 0:
            query += ", \n"
        if c in threshold:
            query += "SUM(%s.%s) %s " % (new_table, column, column)
            header += "%s " % JOIN_TOKEN
            header += "%s.%s %d\n" % (table, column, c)
            continue
        if params != "":
            params += ', '
        if column == column_name:
            if alias is None or alias == "":
                alias = column
            query += "%s AS %s" % (formula, alias)
            params += alias
        else:
            query += " %s.%s" % (new_table, column)
            params += " %s.%s" % (new_table, column)

        header += "%s " % JOIN_TOKEN
        header += "%s.%s %d \n" % (table, column, c)

    old_header, inner_sql = extract_header(sql)

    query += "\n FROM (%s) %s \n" % (inner_sql, new_table)
    query += "GROUP BY %s\n" % params
    query += "ORDER BY %s" % params
    query = header + query

    return query


def add_xml_header(contents):
    """
    Add xml header.

    :param contents:
    :return:
    """
    header = "<!--\n"
    header += "\tThis file is generated with Lod4Stat.\n"
    header += "\tCopyright (C) 2014 Provincia autonoma di Trento\n\n"
    header += "\tREGARDS: Some obsValue has been omitted in order "
    header += "to preserve the statical secret.\n"
    header += "-->\n\n"
    new_contents = header + contents
    return new_contents


def add_sh_like_header(contents):
    """
    Add shell like header.

    :param contents:
    :return:
    """
    header = "# This file is generated with Lod4Stat.\n"
    header += "# Copyright (C) 2014 Provincia autonoma di Trento\n"
    header += "#\n"
    header += "#REGARDS: Some obsValue has been omitted in order "
    header += "to preserve the statical secret.\n"
    header += "\n\n"
    new_contents = header + contents
    return new_contents


def execute_query_on_django_db(query):
    """
    Execute a query on the django database.

    :param query: Sql query.
    :return: Rows, sql result set.
    """
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


def get_table_metadata_value(table_name, key):
    """
    Get the value of metadata by key.

    :param table_name: Table name.
    :param key: Key name.
    :return: The rows result set.
    """
    query = "SELECT value from %s " % METADATA
    query += "WHERE table_name='%s' " % table_name
    query += "and key='%s' " % key
    rows = execute_query_on_django_db(query)
    return rows


def get_metadata_on_column(table_name, column_name):
    """
    Get all metadata on column table.

    :param table_name: Table name.
    :param column_name: Column name.
    :return: The rows result set.
    """
    query = "SELECT * from %s " % METADATA
    query += "WHERE table_name='%s' " % table_name
    query += "and column_name='%s' " % column_name
    query += "and key LIKE 'http://%';"
    rows = execute_query_on_django_db(query)
    return rows


def get_subject_table(table):
    """
    Get SDMX subject of the table.

    :param table: The table name.
    :return: The subject
    """
    query = "SELECT value from %s " % METADATA
    query += "WHERE key='%s' " % SUBJECT
    query += "and table_name='%s';" % table
    rows = execute_query_on_django_db(query)
    return rows


def get_table_description(table):
    """
    Return the description of the table.
    The table description are stored as metadata dcterms description.

    :param table: The table name.
    :return: The table description in natural language.
    """
    query = "SELECT value from %s " % METADATA
    query += "WHERE column_name = 'NULL' "
    query += "and key='%s' " % DESCRIPTION
    query += "and table_name='%s';" % table
    rows = execute_query_on_django_db(query)
    if rows is not None:
        for row in rows:
            return "%s" % row[0]
    return ""


def column_position_in_dataframe(column_description, data_frame):
    """
    Get the column position in dataframe.

    :param column_description:
    :param data_frame:
    :return:
    """
    for c, col_name in enumerate(data_frame.columns):
        if col_name == column_description:
            return c
    return -1


def get_column_description(table_name, column):
    """
    Return the description of the column.
    The column description are stored in the table "colonne".

    :param column: The column name.
    :return: The column description in natural language.
    """
    query = "SELECT value from %s " % METADATA
    query += "WHERE key='%s' " % DESCRIPTION
    query += "and table_name='%s' " % table_name
    query += "and column_name='%s';" % column
    rows = execute_query_on_django_db(query)
    if rows is not None:
        for row in rows:
            val = "%s" % row[0]
            return val.strip()
    return None


def get_table_schema(table_name):
    """
    Get table schema of the table.

    :param table_name: Table name.
    :return: Table schema.
    """
    explorer_connection = connections[EXPLORER_CONNECTION_NAME]
    cursor = explorer_connection.cursor()
    introspection = explorer_connection.introspection
    table_schema = introspection.get_table_description(cursor, table_name)
    return table_schema


def insert_random_data(table_name, rows, min_value, max_value):
    """
    Insert random data i the table.
    The table will be populated with random number.

    :param table_name: name of the table
    :param rows: number of the line to be appended.
    :param min_value: the min integer value for the random number generation.
    :param max_value: the max integer value for the random number generation.
    """
    cursor = connection.cursor()
    introspection = connection.introspection
    table_schema = introspection.get_table_description(cursor, table_name)
    i = 0
    while i < rows:
        col_counter = 0
        columns = ""
        values = ""
        for field in table_schema:
            col_counter += 1
            if field.name == 'id':
                continue
            columns += str(field.name)
            values += str(random.randint(min_value, max_value))
            if col_counter != len(table_schema):
                columns += ','
                values += ','

        query = "INSERT INTO %s (%s) VALUES (%s);" % (
            table_name, columns, values)

        cursor.execute(query)
        i += 1


def delete_all(table_name):
    """
    Delete all data in the table.

    :param table_name: Table name.
    """
    cursor = connection.cursor()
    query = "DELETE FROM %s;" % table_name
    cursor.execute(query)


def email_manual_request_notification(instance, email):
    """
    Send an email notifying the user that the system is handling the request.

    :param instance: manual request instance.
    :param email: Email address to send the email.
    """
    manual_request_id = str(instance.pk)
    subject = _("The statistical service")
    subject += " " + _("is handling")
    subject += " " + _("the request") + " 'l4s" + manual_request_id + "'"
    subject += " " + _("with subject") + " '%s'" % instance.subject
    message = subject + "\n" + _(
        "Your request will be dispatched as soon as possible") + "."
    send_mail(subject, message, DEFAULT_FROM_EMAIL,
              [email], fail_silently=False)


def email_new_manual_request(instance):
    """
    Notify to all the user with flag is_manual_request_dispatcher that
    an user has done a new manual request.

    :param instance: manual request instance.
    """
    manual_request_id = str(instance.pk)
    to_list = []
    objects = User.objects.filter(is_manual_request_dispatcher=True)
    for obj in objects:
        to_list.append(obj.email)

    subject = "[Lod4Stat] " + _("new manual request")
    subject += " " + _("with id") + " 'l4s" + manual_request_id + "'"
    message = "[Lod4Stat] " + _("new manual request")

    current_site = Site.objects.get_current()
    url = 'http://' + current_site.domain + '/manual_request_view/?id=' \
          + manual_request_id
    message += "\n" + url
    send_mail(subject, message, DEFAULT_FROM_EMAIL, to_list,
              fail_silently=False)


def build_queries_to_tables_mapping(queries):
    """
    Build a dictionary with key the query pk and as value the table name used.

    :param queries: Query model list.
    :return: Queries to tables mapping dictionary.
    """
    ret = dict()
    for query in queries:
        m = re.findall(r'%s (.+?)\.' % JOIN_TOKEN, query.sql, re.DOTALL)
        table_list = list(set(m))
        if len(table_list) != 0:
            ret[query.pk] = list(set(m))[0]
    return ret


def build_topic_keywords():
    """
    Build a dictionary with key the topic and value the list of keywords.

    :return: Dictionary to join the topic to keywords.
    """
    topic_keywords = dict()
    query = "SELECT topic, keyword \n"
    query += "FROM topics_keywords;"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            topic = row[0]
            keyword = row[1]
            if not topic in topic_keywords:
                topic_keywords[topic] = []
            topic_keywords[topic].append(keyword)
    return topic_keywords


def build_topic_icons():
    """
    Build a dictionary with key the topic and value the icon to be used.

    :return: Dictionary to join the topic to keywords.
    """
    topic_keywords = dict()
    query = "SELECT topic, image_path \n"
    query += "FROM topics_images;"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            topic = row[0]
            path = row[1]
            topic_keywords[topic] = path
    return topic_keywords


def build_description_table_dict(tables):
    """
    Build a dictionary <table_name, description>.

    :param tables: List of table names.
    :return: A dictionary with descriptions.
    """
    table_str = "'" + "','".join(tables) + "'"
    query = "SELECT table_name, value from %s " % METADATA
    query += "WHERE column_name = 'NULL' "
    query += "and key='%s' " % DESCRIPTION
    query += "and table_name IN (%s);" % table_str

    rows = execute_query_on_django_db(query)
    table_description_dict = dict()

    if not rows is None:
        for row in rows:
            table_name = row[0]
            description = row[1]
            table_description_dict[table_name] = description

    return table_description_dict


def build_description_column_dict(table_name, table_schema, fk):
    """
    Build a dictionary <column_name, description>
    for the specified table schema.

    :param table_name: Table name.
    :param table_schema: Table schema.
    :return: dictionary with column description
    """
    foreign_keys = build_foreign_keys(table_name)

    #print "foreign_keys ", foreign_keys

    table_description_dict = dict()
    for f, field in enumerate(table_schema):

        nome_tabella = table_name
        nome_campo = field.name

        if fk == True:
            if field.name in foreign_keys:
                #print foreign_keys[field.name][0]
                #print foreign_keys[field.name][1]

                nome_tabella = foreign_keys[field.name][0]
                #print "nome_tabella " , nome_tabella
                nome_campo = find_table_description_column(nome_tabella)

        description = get_column_description(nome_tabella, nome_campo)

        if description is not None:
            value = dict()
            value['name'] = field.name
            value['description'] = description
            table_description_dict[f] = value

    return table_description_dict


class Variable(object):
    """ Object used to store variable info. """

    def __init__(self, variable_name=None, variable_type=None):
        self.name = variable_name
        self.type = variable_type
        self.default_value = ""
        self.widget = 'INPUT'

    def get_default_value(self):
        return self.default_value

    def get_type(self):
        return self.type

    def get_name(self):
        return self.name

    def get_widget(self):
        return self.widget

    def set_default_value(self, value):
        self.default_value = value

    def set_widget(self, widget):
        self.widget = widget


def get_params_dictionary(variable_dictionary):
    """
    Take the variable dictionary and return a dictionary variable->value
    compliant with the structure used by sql explorer.
    This is used when a parametric query has been called without pass
    query args with uri.

    :param variable_dictionary: Variable dictionary.
    :return: The parameters dictionary.
    """
    if variable_dictionary is None:
        return None

    params_dictionary = dict()

    for key in variable_dictionary.iterkeys():
        variable_dictionary_key_item = variable_dictionary.get(key)
        variable_name = variable_dictionary_key_item.get_name()
        variable_values = variable_dictionary_key_item.get_default_value()
        params_dictionary[variable_name] = variable_values

    return params_dictionary


def get_types_dictionary(variable_dictionary):
    """
    Take the type dictionary and return a dictionary with types.
    The dictionary will be passed on query with post.

    :param variable_dictionary: Variable dictionary.
    :return: the type dictionary: Type dictionary.
    """
    if variable_dictionary is None:
        return None

    types_dictionary = dict()

    for key in variable_dictionary.iterkeys():
        variable_dictionary_key_item = variable_dictionary.get(key)
        variable_name = variable_dictionary_key_item.get_name()
        variable_values = variable_dictionary_key_item.get_type()
        types_dictionary[variable_name] = variable_values

    return types_dictionary


def get_widgets_dictionary(variable_dictionary):
    """
    Take the widgets dictionary and return a dictionary with widgets.
    The dictionary will be passed on query with post.

    :param variable_dictionary: Variable dictionary.
    :return: The widgets dictionary.
    """
    if variable_dictionary is None:
        return None

    widgets_dictionary = dict()

    for key in variable_dictionary.iterkeys():
        variable_dictionary_key_item = variable_dictionary.get(key)
        variable_name = variable_dictionary_key_item.get_name()
        variable_values = variable_dictionary_key_item.get_widget()
        widgets_dictionary[variable_name] = variable_values

    return widgets_dictionary


def notify_error(token, expected_token):
    """
    Return an error message for the invalid line.

    :param token: The invalid token to notify to the user.
    :param expected_token: The expected token instead of the wrong one.
    :return: The message containing the error to show by exception.
    """
    invalid_syntax = _('Invalid syntax: ')
    usage = _('Usage')
    usage += '%s <VARIABLE> {<QUERY}' % DECLARE_TOKEN
    usage += "\n\t" + '%s <VARIABLE> {<QUERY}' % SET_TOKEN
    usage += "\n\t" + '%s <INPUT|SELECT|MULTISELECT>' % WIDGET_TOKEN
    error_msg = _('Invalid Token ') + token + "; "
    error_msg += _(' expected token is ') + expected_token
    return invalid_syntax + "\n" + usage + "\n" + error_msg


def parse_widget(words, variable_dictionary):
    """
    Parse a string containing the widget choose by user for a specific
    variable used in a parametric sql query.

    :param words: List of words.
    :param variable_dictionary: The variable dictionary.
    """
    variable_name = words[1]
    widget = words[2]
    variable_dictionary[variable_name].set_widget(widget)


def get_variable_dictionary(query):
    """
    Get a variable dictionary containing the variable name, type and value.
    If the grammar is incorrect return directly a meaningful error also.

    :param query: The explorer query.
    :return: The variable dictionary.
    """
    variable_dictionary = dict()

    for line in query.sql.splitlines():
        lstripped_line = line.lstrip(' ')
        words = lstripped_line.split(' ')
        declare = words[0]
        if declare == WIDGET_TOKEN:
            parse_widget(words, variable_dictionary)
            continue

        if declare != DECLARE_TOKEN and declare != SET_TOKEN:
            continue

        keyword = words.pop(0)
        variable_name = words.pop(0)

        internal_query = " ".join(words)
        token = internal_query[0]
        # Variable type is a query between braces.
        expected_token = '{'
        if token != expected_token:
            return None, notify_error(token, expected_token)

        token = internal_query[len(internal_query) - 1]
        expected_token = '}'
        if token != expected_token:
            return None, notify_error(token, expected_token)

        # Remove braces.
        internal_query = internal_query[:0] + internal_query[1:]
        internal_query = internal_query[:-1]

        try:
            rows = execute_query_on_main_db(internal_query)
        except Exception, e:
            return variable_dictionary, str(e)

        values = []
        for row in rows:
            value = "%s" % row[0]
            values.append(value)

        if keyword == DECLARE_TOKEN:
            var = Variable(variable_name, values)
            variable_dictionary[variable_name] = var

        else:
            variable_dictionary[variable_name].set_default_value(values)

    return variable_dictionary, None


def url_get_parameters(request):
    """
    Get the json parameter and hack it in order to use list instead of string.

    :param request: The Django request.
    :return: The query parameters.
    """
    params = url_get_params(request)
    if params is not None:
        for variable_name in params:
            if isinstance(params[variable_name], list):
                continue
            params[variable_name] = [params[variable_name]]

    return params


def get_key_column_value(table_name, column_name, key_name):
    """
    Get metadata with key key_name for table table_name and column column_name.

    :param table_name: The table name.
    :param column_name: The column name.
    :param key_name: The key name.
    :return: The value of the key on column table.
    """
    metadata_list = Metadata.objects.filter(table_name=table_name)

    if column_name is not None:
        metadata_list = metadata_list & Metadata.objects.filter(
            column_name=column_name)

    metadata_list = metadata_list & Metadata.objects.filter(key=key_name)

    if metadata_list is not None:
        for metadata in metadata_list:
            return metadata.value

    return None


def get_key_column_values(table_name, column_name, key_name):
    """
    Get metadata with key key_name for table table_name and column column_name.

    :param table_name: The table name.
    :param column_name: The column name.
    :param key_name: The key name.
    :return: The value of the key on column table.
    """
    val = []
    metadata_list = Metadata.objects.filter(table_name=table_name)

    if column_name is not None:
        metadata_list = metadata_list & Metadata.objects.filter(
            column_name=column_name)

    metadata_list = metadata_list & Metadata.objects.filter(key=key_name)

    if metadata_list is not None:

        for metadata in metadata_list:
            val.append(metadata.value)

    return val


def get_key_table_value(table_name, key_name):
    """
    Get key value for metadata on table.

    :param table_name: The table name.
    :param key_name: The key name.
    :return: The value of key on table.
    """
    metadata_list = Metadata.objects.filter(table_name=table_name,
                                            key=key_name,
                                            column_name="NULL")

    if metadata_list is not None:
        for metadata in metadata_list:
            return metadata.value

    return None


def is_secret(table_name, column_name):
    """
    Return if a table' column must be protected by statistical secret.

    :param table_name: Table name.
    :param column_name: Column name.
    :return: If the column on table is secret.
    """
    value = get_key_column_value(table_name, column_name, 'secret')

    if value is not None and value == "TRUE":
        return True

    return False


def get_constraint(table_name, column_name):
    """
    Return if a table' column must be protected
    by a constraint on a  referenced table.

    :param table_name: Table name.
    :param column_name: Column name.
    :return: The constraint value.
    """
    value = get_key_column_value(table_name, column_name, 'constraint')

    if value is not None:
        return value

    return None


def get_threshold(table_name, column_name):
    """
    Get statistical secret threshold.

    :param table_name: Table name.
    :param column_name: Column name.
    :return: The threshold value.
    """

    return get_key_column_value(table_name, column_name, 'threshold')


def is_decoder_table(table_name):
    """
    Return if the table is a decoder table

    :param table_name: Table name.
    :return: If the table is a decoder one.
    """

    value = get_key_table_value(table_name, 'decoder')
    if value is not None and value == "TRUE":
        return True
    return False


def filter_coder_table(tables):
    """
    Get a list of tables and return the subset containing the tables in input
    that don't belong to the decoder table set.

    :param tables:
    :return:
    """
    table_names = "'" + "','".join(tables) + "'"
    query = "SELECT table_name FROM web_metadata \n"
    query += "WHERE key='decoder' and value='TRUE' \n"
    query += "and table_name IN(%s)" % table_names
    rows = execute_query_on_django_db(query)
    decoder_tables = []
    if rows is not None:
        for row in rows:
            decoder_tables.append(row[0])
    coder_tables = []
    for table_name in tables:
        if not table_name in decoder_tables:
            coder_tables.append(table_name)

    return coder_tables


def all_visible_tables(request):
    """
    Get the list of the tables visible in query editor.

    :return: List of table names.
    """
    query = "SELECT DISTINCT(table_name) FROM web_metadata \n"

    if request.user.is_staff:
      query += "WHERE upper(key)='VISIBLE' and upper(value)='TRUE' \n"

    query += "order by table_name"

    rows = execute_query_on_django_db(query)
    ret_tables = []
    if rows is not None:
        for row in rows:
            ret_tables.append(row[0])
    return ret_tables


def exclude_invisible_tables(tables):
    """
    Exclude to the input set the tables not visible in the query editor.

    :param tables:
    :return:
    """
    table_names = "'" + "','".join(tables) + "'"
    query = "SELECT DISTINCT(table_name) FROM web_metadata \n"
    query += "WHERE upper(key)='VISIBLE' and upper(value)='TRUE' "
    query += "and table_name IN(%s)" % table_names
    print "query " , query
    rows = execute_query_on_django_db(query)
    ret_tables = []
    if rows is not None:
        for row in rows:
            ret_tables.append(row[0])

    return ret_tables


def get_concept(value):
    """
    Get concept.

    :param value:
    :return: Concept.
    """
    query = "SELECT table_name, column_name from %s " % METADATA
    query += "WHERE column_name != 'NULL' and "
    query += "key = '%s' and " % CONCEPT
    query += "value = '%s'" % value
    rows = execute_query_on_django_db(query)
    if rows is not None and len(rows) > 0:
        return rows[0][0], rows[0][1]
    return None, None


def located_in_area(table, column, value):
    """
    Get locatedInArea metadata on column table.

    :param table: Table name.
    :param column: Column name.
    :return: The table.column that contains the super area.
    """
    values = get_key_column_values(table, column, LOCATED_IN_AREA)
    for val in values:
        if value == val:
            ref_table, ref_column = get_concept(value)
            return ref_table, ref_column
    return None, None


def located_in_area_value_to_column(metadata_list):
    """
    Return hashtable with key the value of LOCATED_IN_AREA key
    and value the column table referenced by link.

    :param metadata_list:
    :return: <value,[table,column]>
    """
    ret = dict()
    metadata_list = metadata_list.filter(key=LOCATED_IN_AREA)
    for metadata in metadata_list:
        value = metadata.value
        table = metadata.table_name
        column = metadata.column_name
        ref_table, ref_column = located_in_area(table, column, value)
        ret[value] = [ref_table, ref_column]

    return ret


def get_dimensions(table, column):
    """
    Get dimension of table column name.

    :param table: Table name.
    :param column: Column name.
    :return: Dimension.
    """
    return get_key_column_values(table, column, DIMENSION)


def get_measure(table, column):
    """
    Get measure of table column name.

    :param table: Table name.
    :param column: Column name.
    :return: Measure.
    """
    return get_key_column_values(table, column, MEASURE)


def is_obs_value(table, column):
    """
    Return if the column table contains an observable value.

    :param table: Table name.
    :param column: Column name.
    :return: Boolean.
    """
    values = get_measure(table, column)
    for val in values:
        if val == OBS_VALUE:
            return True
    return False


def is_ref_period(table, column):
    """
    Return if the column table is a time reference period.

    :param table: Table_name.
    :param column: Column name.
    :return: Boolean.
    """
    values = get_dimensions(table, column)
    for val in values:
        if val == REF_PERIOD:
            return True
    return False


def is_ref_area(table, column):
    """
    Return if the column table is a referenced geographical area.

    :param table: Table name.
    :param column: Column name.
    :return: Boolean.
    """
    values = get_dimensions(table, column)
    for val in values:
        if val == REF_AREA:
            return True
    return False


def get_all_aggregations(table_name):
    """
    Get all the aggregation that are feasible on table.

    :param table_name: Table name.
    :return: Boolean.
    """
    agg = dict()
    agg_values = dict()
    fks = build_foreign_keys(table_name)

    #print "fks ", fks

    for col in fks:
        tup = fks[col]

        ref_tab = tup[0]
        ref_col = tup[1]

        query = "SELECT b.id, b.table_name, b.column_name, "
        query += "d.table_name, d.column_name \n"
        query += "FROM web_metadata b \n"
        query += "JOIN web_metadata d ON( \n"
        query += "b.key='%s' \n" % LOCATED_IN_AREA
        query += "and b.table_name='%s' \n" % ref_tab
        query += "and b.column_name = '%s' \n" % ref_col
        query += "and d.key = '%s' \n" % CONCEPT
        query += "and d.value = b.value \n"
        query += ")"

        #print "query " , query

        rows = execute_query_on_django_db(query)

        if not rows is None:
            for row in rows:
                pk = row[0]

                ref_tab = tup[0]
                ref_col = tup[1]

                ref_col = find_table_description_column(ref_tab)

                if ref_col is None or ref_col == "":
                    src_description = col
                else:
                    src_description = get_column_description(ref_tab, ref_col)
                    if src_description is None or src_description == "":
                        src_description = ref_col

                #print "src_description ", src_description

                ref_tab = row[3]
                ref_col = row[4]

                foreign_keys = build_foreign_keys(ref_tab)

                #print "foreign_keys ", foreign_keys

                if foreign_keys is None:
                    ref_description = get_column_description(ref_tab, ref_col)
                else:
                    #print "foreign_keys ", foreign_keys
                    for col2 in foreign_keys:
                        if col2 == row[4]:
                            #print "col2 ", col2

                            tup2 = foreign_keys[col2]
                            ref_tab = tup2[0]
                            #ref_col = tup2[1]

                            ref_col = find_table_description_column(ref_tab)
                            ref_description = get_column_description(ref_tab, ref_col)

                #print "ref_description ", ref_description

                if ref_description is None or ref_description == "":
                    ref_description = ref_col

                if not src_description in agg:
                    agg[src_description] = dict()

                agg[src_description][pk] = ref_description

                """
                print bcolors.OKBLUE
                print "agg1 ", agg
                print "ref_tab " ,ref_tab
                print "ref_col " , ref_col
                print "table_name " , table_name
                print "tup[0] " , tup[0]
                print "tup[1] " , tup[1]
                print "row[3] " , row[3]
                print "row[4] " , row[4]
                print bcolors.ENDC
                """

                vals = get_all_field_values(row[3], row[4], None)

                #print "vals " , vals

                agg_values[pk] = vals

    #print "agg1 ", agg

    metadata_list = Metadata.objects.filter(table_name=table_name,
                                            key=CLASS)

    #print "metadata_list " , metadata_list

    for m, metadata in enumerate(metadata_list):  # tipo per ATECO dove c'e' una formula
        column_name = metadata.column_name
        src_description = get_column_description(table_name, column_name)
        if src_description is None or src_description == "":
            src_description = column_name
        if not src_description in agg:
            agg[src_description] = dict()
        value = "%s" % metadata.value
        ref_description = re.findall(r'AS(.*)', value)[0].strip().strip('"')
        if ref_description is None or ref_description == "":
            ref_description = column_name + "_" + str(m+1)
        agg[src_description][metadata.pk] = ref_description
        formula, alias = get_class_formula_alias(metadata.pk)
        vals = get_all_field_values(table_name, column_name, formula)
        pk = ast.literal_eval(metadata.pk)
        agg_values[pk] = vals

    #print "table_name ", table_name
    #print "agg2 ", agg

    return agg, agg_values


def get_aggregations(cols):
    """
    Get the aggregation that are feasible on query passing involved columns.

    :param cols: Columns.
    :return: All feasible aggregations on columns.
    """
    agg = dict()

    for c, col in enumerate(cols):
        col_item = cols[col]
        table_name = col_item['table']
        column_name = col_item['column']
        src_description = get_column_description(table_name, column_name)
        if src_description is None or src_description == "":
            src_description = column_name
        metadata_list = Metadata.objects.filter(table_name=table_name,
                                                column_name=column_name,
                                                key=LOCATED_IN_AREA)

        for metadata in metadata_list:
            ref_tab, ref_col = located_in_area(metadata.table_name,
                                               metadata.column_name,
                                               metadata.value)
            if ref_tab is not None and ref_col is not None:
                ref_description = get_column_description(ref_tab, ref_col)
                if ref_description is None or ref_description == "":
                    ref_description = ref_col
                if not src_description in agg:
                    agg[src_description] = dict()
                agg[src_description][metadata.pk] = ref_description

        metadata_list = Metadata.objects.filter(table_name=table_name,
                                                column_name=column_name,
                                                key=CLASS)
        for m, metadata in enumerate(metadata_list):
            src_description = get_column_description(table_name, column_name)
            if src_description is None or src_description == "":
                src_description = column_name
            if not src_description in agg:
                agg[src_description] = dict()
            value = "%s" % metadata.value
            ref_description = re.findall(r'AS(.*)', value)[0].strip().strip(
                '"')
            if ref_description is None or ref_description == "":
                ref_description = column_name + "_" + str(m+1)
            agg[src_description][metadata.pk] = ref_description

    return agg


def drop_total_column(data_frame):
    """
    Drop the total column in data frame.

    :param data_frame:
    :return: Data frame.
    """
    col = data_frame.columns[len(data_frame.columns)-1]
    if isinstance(col, tuple):
        last_col = col[len(col)-1]
    else:
        last_col = col
    if last_col != TOTAL:
        return data_frame
    data_frame = data_frame.drop(col, axis=1)

    return data_frame


def drop_total_row(data_frame):
    """
    Drop the total row in data frame.

    :param data_frame:
    :return: Data frame.
    """
    try:
        index = data_frame.index.get_loc(TOTAL)
        data_frame = data_frame.drop(data_frame.index[index], axis=0)
    except (KeyError, TypeError):
        index = data_frame.index[len(data_frame.index)-1]
        first_index_part = index
        if first_index_part != TOTAL:
            return data_frame
        data_frame = data_frame.drop(index, axis=0)

    return data_frame


def limit_data_frame(df):
    """
    Limit data frame size for the preview depending on settings.

    :param df: Panda data frame.
    :return: The limited Panda data frame.
    """
    index_len = len(df.index.names)
    if len(df.columns) > EXPLORER_DEFAULT_COLS:
        df = df.drop(df.columns[EXPLORER_DEFAULT_COLS - index_len - 1:len(
            df.columns) - 1], axis=1)
    if len(df.index) > EXPLORER_DEFAULT_ROWS:
        df = df.drop(df.index[EXPLORER_DEFAULT_ROWS - 1:len(df.index) - 1])
    return df


def get_column_desc(column_description, f):
    """
    Get the description on the field name.

    :param column_description: hash table with all fields description.
    :param f: name
    :return: description.
    """
    for c in column_description:
        val = column_description[c]
        if val['name'] == f:
            description = "%s" % val['description']
            description = description.strip()
            return description

    return f


def get_aggregation_descriptions(agg_id):
    """
    Get the id of aggregation and return source description and
    target description.

    :param agg_id: Aggregation id.
    :return: Description (from, to).
    """
    metadata = Metadata.objects.get(id=agg_id)
    ref_table, ref_column = located_in_area(metadata.table_name,
                                            metadata.column_name,
                                            metadata.value)
    src_col_desc = get_column_description(metadata.table_name,
                                          metadata.column_name)
    target_col_desc = get_column_description(ref_table, ref_column)
    return src_col_desc, target_col_desc


def build_summarize_filters(column_description,
                            values,
                            filters,
                            columns_names,
                            index_names,
                            hidden_fields):
    """
    Build an hash table that summarize the filters.

    :param column_description: Column description hash table.
    :param values: Values.
    :param filters: Filters.
    :param columns_names: Column names.
    :param index_names: Indices names.
    :param hidden_fields: Hidden fields.
    """
    ret = dict()
    for f in filters:
        if f in hidden_fields.values():
            continue
        filt = filters[f]
        col_desc = get_column_desc(column_description, f)
        if len(filt) == len(values[f]):
            vect = []
            if f in index_names or f in columns_names:
                vect.append(ALL)
                ret[col_desc] = vect
            else:
                vect.append(TOTAL)
                ret[col_desc] = vect
        elif len(filt) > 0:
            vect = []
            for v, val in enumerate(filt):
                val_desc = "%s" % val[1]
                val_desc = val_desc.strip()
                vect.append(val_desc)
            ret[col_desc] = vect
    return ret


def build_agg_summarize_filters(target_col_desc,
                                all_vals,
                                filters):
    """
    Build an hash table that summarize the aggregation filters.

    :param target_col_desc: Column description.
    :param all_vals: All possible values that can have the aggregated field.
    :param filters: Filters.
    :return:
    """
    ret = dict()
    vect = []
    if len(all_vals) == len(filters):
        vect.append(ALL)
    else:
        for v, val in enumerate(filters):
            val_desc = "%s" % val[1]
            val_desc = val_desc.strip()
            vect.append(val_desc)
    ret[target_col_desc] = vect
    return ret


def build_aggregation_title(agg_id, aggregations):
    """
    Get aggregation title.

    :param agg_id: Aggregation metadata id.
    :param aggregations: All aggregations
    :return: Title.
    """
    group_by = unicode(_("group by"))

    for agg in aggregations:
        val = aggregations[agg]
        if agg_id in val:
            src_col_desc = agg
            target_col_desc = val[agg_id]
            title = "%s %s " % (src_col_desc, group_by)
            title += "%s" % target_col_desc.lower()
            return title, src_col_desc, target_col_desc

    return "", "", ""


def build_all_filter(column_description,
                     values,
                     filters,
                     aggregation_ids,
                     agg_values,
                     agg_filters,
                     agg_col_desc,
                     columns_names,
                     index_names,
                     hidden_fields,
                     not_sel_aggregations_ids,
                     not_agg_selection_value,
                     aggregations):
    """
    Build a table with all filters.

    :param columns_names: Column names.
    :param column_description: Column descriptions.
    :param values: Values.
    :param filters: Filters.
    :param aggregation_ids: List of aggregation ids.
    :param agg_values: Hash of aggregations values.
    :param agg_filters: Hash of aggregations filters.
    :param agg_col_desc: Hash of description columns for aggregations.
    :param index_names: Indices names.
    :param hidden_fields:
    :return: filters.
    """

    ret = {}
    for a, a_id in enumerate(aggregation_ids):
        target_col_desc = agg_col_desc[a_id]['description']
        u_id = unicode(a_id)
        summarize_agg_filters = build_agg_summarize_filters(target_col_desc,
                                                            agg_values[a_id],
                                                            agg_filters[u_id])
        ret.update(summarize_agg_filters)

    for a, a_id in enumerate(not_sel_aggregations_ids):
        u_id = unicode(a_id)

        agg_title, src_desc, agg_desc = build_aggregation_title(a_id,
                                                                aggregations)

        summarize_agg_filters = build_agg_summarize_filters(agg_desc,
                                                            agg_values[a_id],
                                                            not_agg_selection_value[u_id])
        ret.update(summarize_agg_filters)



    summarize_filters = build_summarize_filters(column_description,
                                                values,
                                                filters,
                                                columns_names,
                                                index_names,
                                                hidden_fields)
    ret.update(summarize_filters)

    return ret


def build_query_summary(column_description,
                        values,
                        filters,
                        aggregation_ids,
                        aggregations,
                        agg_values,
                        agg_filters,
                        columns_names,
                        index_names,
                        hidden_fields,
                        not_sel_aggregations_ids,
                        not_agg_selection_value):
    """
    Build a query summary with an hash table with aggregation, description
    and target column an a table with column descriptions, selected values.

    :param columns_names: Column names.
    :param index_names: Indices names.
    :param column_description: Structure with column description.
    :param values: All values for each field.
    :param filters:  Filter on query.
    :param aggregation_ids: List of identifiers for aggregations.
    :param aggregations: All the availbale aggregations.
    :param agg_values: All values for aggregated fields.
    :param agg_filters Filter on aggregated fields.
    :param index_names:: Field used ad columns
    :param index_names:: Field used ad indices.
    :param hidden_fields: Hidden fields.
    :return: Dictionary with description.
    """

    #print "aggregation_ids ", aggregation_ids
    #print "aggregations ", aggregations

    agg_col_desc = dict()
    for a, agg_id in enumerate(aggregation_ids):
        agg_title, src_desc, agg_desc = build_aggregation_title(agg_id,
                                                                aggregations)

        val = dict()
        val['title'] = agg_title
        val['description'] = agg_desc
        val['column'] = src_desc
        agg_col_desc[agg_id] = val

    #print "agg_col_desc ", agg_col_desc

    sel_tab = build_all_filter(column_description,
                               values,
                               filters,
                               aggregation_ids,
                               agg_values,
                               agg_filters,
                               agg_col_desc,
                               columns_names,
                               index_names,
                               hidden_fields,
                               not_sel_aggregations_ids,
                               not_agg_selection_value,
                               aggregations)

    return agg_col_desc, sel_tab


def build_query_desc(agg_col_desc, sel_tab):
    """
    Take build query summary and return a summary in printable format.

    :param agg_col_desc: Hash of description columns for aggregation.
    :param sel_tab:
    :return: Sql query containing descriptions.
    """
    description = ""
    for agg in agg_col_desc:
        agg_title = agg_col_desc[agg]['title']
        description += "%s\n" % agg_title

    if len(agg_col_desc) == 0:
        description += "%s" % unicode(_("Selected values"))

    for key in sel_tab:
        value = ", ".join(sel_tab[key])
        description += "\n%s: %s" % (key, value)

    return description


def build_query_title(column_description, obs_values, agg_col, cols, rows):
    """
    Build a title for the data frame taking the columns and indices.

    :param column_description:
    :param obs_values:
    :param cols:
    :param rows:
    """
    for_s = unicode(_("for"))
    and_s = unicode(_("and"))
    title = ""

    for o, obs_value in enumerate(obs_values):
        desc = get_column_desc(column_description, obs_value)
        if o == 0:
            title = desc[0].upper() + desc[1:]
        else:
            title += ", " + desc.lower()

    title += " %s " % for_s

    for c, col in enumerate(cols):
        desc = get_column_desc(column_description, col)
        for agg_id in agg_col:
            src_col_desc = agg_col[agg_id]["column"]
            if src_col_desc == desc:
                desc = agg_col[agg_id]["description"]

        if c == 0:
            title += desc.lower()
        else:
            title += ", " + desc.lower()

    for r, row in enumerate(rows):
        desc = get_column_desc(column_description, row)
        for agg_id in agg_col:
            src_col_desc = agg_col[agg_id]["column"]
            if src_col_desc == desc:
                desc = agg_col[agg_id]["description"]

        if r == len(rows)-1:
            title += " %s " % and_s
            title += desc.lower()
        else:
            title += ", " + desc.lower()

    return title


def get_data_frame_first_index(data_frame):
    """
    Get the first data frame index; if it is multi-index is returned index at
    level zero else is returned the unique index.

    :param data_frame: Data frame.
    :return: The index.
    """
    if has_data_frame_multi_level_index(data_frame):
        return data_frame.index.levels[0]

    return data_frame.index


def get_data_frame_first_column(data_frame):
    """
    Get the first data frame index; if it is multi-index is returned index at
    level zero else is returned the unique index.

    :param data_frame: Data frame.
    :return: The index.
    """
    if has_data_frame_multi_level_columns(data_frame):
        return data_frame.columns.levels[0]

    return data_frame.columns


def has_data_frame_multi_level_columns(df):
    """
    Check if the data frame has multi level columns.

    :param df: Data frame.
    :return: Boolean.
    """
    return type(df.columns) == pd.MultiIndex


def has_data_frame_multi_level_index(df):
    """
    Check if the data frame has multi level index.

    :param df: Data frame.
    :return: Boolean.
    """
    return type(df.index) == pd.MultiIndex


def to_utf8(v):
    """
    Encode the string in Utf-8. If it is not a string return the simple type
    as is.

    :param v: Param to be converted.
    :return: encoding string in utf-8 or numeric type
    """
    if v is None:
        return "*"
    if isinstance(v, six.string_types):
        return v.encode('utf-8')
    return v


def data_frame_to_html(df, visible, pivot):
    """
    Convert data frame in html language ready to be shown.

    :param df: Data frame.
    :param visible: We must show all the data frame.
    :param pivot: Columns in pivot.
    :return: Html code to be rendered in a html page.
    """
    html = ""
    index_v = False
    if df is not None:
        if pivot is not None and len(pivot) > 0:
            index_v = True
        else:
            df = df.drop(df.columns[0], axis=1)

        if not visible:
            html = df.to_html(classes="table table-striped",
                              index=index_v,
                              max_cols=EXPLORER_DEFAULT_COLS,
                              max_rows=EXPLORER_DEFAULT_ROWS)
        else:
            html = df.to_html(classes="table table-striped",
                              index=index_v)
        #html = html.replace("...", "")
    return html

def save_value(nome_file, stringa):

    nome_file = nome_file + ' ' + datetime.now().strftime("%H:%M:%S.%f")

    f = open(nome_file, 'w')
    f.write(stringa)
    f.close()


