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
Reconciliation functions.
"""
from web.utils import detect_special_columns,\
    build_foreign_keys,\
    execute_query_on_django_db, \
    execute_query_on_main_db, \
    get_column_description,\
    find_table_description_column

RECONCILIATION = 'web_reconciliation'


def build_code_to_url_mapping(fk):
    """
     Build an hash table with key code and reconciled url as value.

    :param fk: Foreign key.
    :return: Dictionary <code, url>.
    """
    code_to_url = dict()
    ref_table = fk[0]
    ref_column = fk[1]
    query = "SELECT code_id, url FROM %s \n" % RECONCILIATION
    query += "WHERE table_name='%s' " % ref_table
    query += "and column_name='%s'" % ref_column
    rows = execute_query_on_django_db(query)
    if not rows is None:
        for row in rows:
            code = row[0]
            url = row[1]
            code_to_url[code] = url
    return code_to_url


def build_desc_to_code_mapping(fk):
    """
    Build an hash table with key description and code as value.

    :param fk:  Foreign key.
    :return: Dictionary <description, code>.
    """
    ret = dict()
    table = fk[0]
    code_column = fk[1]
    desc_column = find_table_description_column(table)
    query = "SELECT %s, %s " % (code_column, desc_column)
    query += "FROM %s" % table
    rows = execute_query_on_main_db(query)
    if not rows is None:
        for row in rows:
            code = row[0]
            desc = row[1]
            ret[desc] = code
    return ret


def reconciles_data_frame(df, sql):
    """
    Reconciles data frame using url instead of descriptions.
    REGARDS: Now this function works only on un-pivoted, plain data frame.

    :param df: Data frame.
    :param sql: The query sql code.
    :return: Reconciled Data frame.
    """
    st = detect_special_columns(sql)
    fks_t = dict()
    code_to_url_col = dict()
    desc_to_code_col = dict()

    for key in st.cols:
        value = st.cols[key]
        column = value['column']
        table = value['table']
        column_desc = get_column_description(table, column)
        if not column_desc in df.columns:
            # It is not used in the query.
            continue
        if not table in fks_t:
            fks = build_foreign_keys(table)
            fks_t[table] = fks
        else:
            fks = fks_t[table]
        if column in fks:
            fk = fks[column]
            code_to_url = build_code_to_url_mapping(fk)
            if len(code_to_url) != 0:
                # It contains some reconciliation rows.
                code_to_url_col[column_desc] = code_to_url
                desc_to_code = build_desc_to_code_mapping(fk)
                desc_to_code_col[column_desc] = desc_to_code

    for n, col_name in enumerate(df.columns):
        if col_name is None or col_name not in code_to_url_col:
            continue

        code_to_url = code_to_url_col[col_name]
        desc_to_code = desc_to_code_col[col_name]
        c_position = df.columns.get_loc(col_name)

        values = df[col_name]
        for v, value in enumerate(values):
            if value in desc_to_code:
                code = desc_to_code[value]
                url = code_to_url[code]
                df.iloc[v, c_position] = url

    return df
