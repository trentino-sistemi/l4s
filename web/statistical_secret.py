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
Routines to preserve statistical secret.
"""
from django.utils.translation import ugettext_lazy as _
import pandas as pd
import numpy as np
from web.utils import get_table_schema, \
    execute_query_on_main_db, \
    build_constraint_query, \
    build_aggregation_query, \
    detect_special_columns, \
    list_obs_value_column_from_dict, \
    get_data_from_data_frame, \
    drop_total_column, \
    drop_total_row, \
    build_description_query, \
    is_dataframe_multi_index, \
    stringify

PRESERVE_STAT_SECRET_MSG = _(
    "Some value are asterisked to preserve the statistical secret")

ASTERISK = '*'


def find_row_with_min_value(data, r_start, r_end, c):
    """
    Find in the data the row with min value in column c from r_start to r_end.

    :param data: The input data from a query result set.
    :param r_start: The start index where start the group.
    :param r_end: The row index where end the group.
    :param c: The column where looking for the minimum value.
    :return: The row with the min value on column c.
    """
    cell = str(data[r_start][c])
    if not cell.startswith(ASTERISK):
        min_row = r_start
        if r_start == r_end:
            return min_row
    else:
        min_row = None

    r = r_start
    while True:
        if r == r_end + 1:
            break

        row = data[r]

        cell = str(row[c])
        if cell.startswith(ASTERISK):
            r += 1
            continue

        if min_row is None or float(data[r][c]) < float(data[min_row][c]):
            min_row = r

        r += 1

    return min_row


def find_row_with_min_value_exclude_zero(data, r_start, r_end, c):
    """
    Find in the data the row with min value in column c from r_start to r_end
    excluding zero.

    :param data: The input data from a query result set.
    :param r_start: The start index where start the group.
    :param r_end: The row index where end the group.
    :param c: The column where looking for the minimum value.
    :return: The row with the min value on column c.
    """
    cell = str(data[r_start][c])
    min_row = None
    if not cell.startswith("*"):
        if float(cell) > 0.0:
            min_row = r_start
        if r_start == r_end:
            return min_row
    else:
        min_row = None

    r = r_start
    while True:
        if r == r_end + 1:
            break

        row = data[r]

        cell = str(row[c])
        if cell.startswith(ASTERISK):
            r += 1
            continue

        if float(data[r][c]) != 0.0:
            if min_row is None or float(data[r][c]) < float(data[min_row][c]):
                min_row = r

        r += 1

    return min_row


def pivot(data, headers, columns, rows, value):
    """
    Pivot the table.

    :param data: List of tuples containing query result set.
    :param headers: Result set header.
    :param columns: Pivot columns.
    :param rows: Pivot rows.
    :param value: Pivot value.
    :return: Data and Pandas data frame.
    """
    if len(data) == 0:
        error = _("I can not pivot the table")
        e_value = _("the query return an empty result set")

        error = "%s: %s" % (unicode(error), unicode(e_value))
        return None, None, error

    df = pd.DataFrame(data, columns=headers)

    try:
        pivot_df = df.pivot_table(columns=columns,
                                  index=rows,
                                  values=value,
                                  margins=True,
                                  aggfunc=np.sum)
    except Exception, e:
        error = _("I can not pivot the table")
        e_value = str(e)
        if e_value == "All objects passed were None":
            e_value = _("All objects passed were None")

        error = "%s: %s" % (unicode(error), unicode(e_value))
        return None, None, error

    pivot_df = pivot_df.applymap(
        lambda a: str(a).replace(".0", "", 1).replace("nan", "0"))

    all_s = unicode(_("All")).encode('ascii')
    pivot_df.rename(columns=lambda x: str(x).replace('.0', ''), inplace=True)
    pivot_df.rename(index=lambda x: str(x).replace('.0', ''), inplace=True)
    pivot_df.rename(columns={'All': all_s}, inplace=True)
    pivot_df.rename(index={'All': all_s}, inplace=True)
    data = get_data_from_data_frame(pivot_df)

    return data, pivot_df, None


def get_threshold_from_dict(threshold_columns_dict, col):
    """
    Get threshold from dictionary.

    :param threshold_columns_dict: Threshold dictionary.
    :param col: Column number.
    :return: The threshold.
    """
    threshold = 3.0
    if col in threshold_columns_dict:
        return threshold_columns_dict[col]

    return threshold


def protect_pivoted_secret(data,
                           threshold_columns_dict,
                           rows,
                           columns,
                           debug):
    """
    Protect statistical secret for pivot table.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: Threshold dictionary.
    :return: The pivot table preserving statistical secret.
    """
    for r, row in enumerate(data, start=len(columns)):
        if r == len(data):
            break
        for c, column in enumerate(row):
            threshold = get_threshold_from_dict(threshold_columns_dict, c)
            val = row[c]
            if str(val).startswith(ASTERISK):
                continue
            if c < len(rows) or c == len(data[0]) - 1:
                continue

            if 0.0 < float(val) < threshold:
                row[c] = ASTERISK
                if debug:
                    row[c] += '(' + val + ")"
    return data


def find_column_with_min_value(data, row):
    """
    Return the index of the column with minimum value.

    :param data: List of tuples containing query result set.
    :param row: The row where found the min value.
    :return: The column index that point to the row cell with minimum value.
    """
    min_index = None
    for c, column in enumerate(row):
        if c == len(data[0]) - 1:
            continue
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if min_index is None or float(row[c]) < float(row[min_index]):
            min_index = c

    return min_index


def find_column_with_min_value_exclude_zero(data, row):
    """
    Return the index of the column with minimum value excluding zero.

    :param data: List of tuples containing query result set.
    :param row: The row where found the min value.
    :return: The column index that point to the row cell with minimum value.
    """
    min_index = None
    for c, column in enumerate(row):
        if c == len(data[0]) - 1:
            continue
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if float(row[c]) != 0.0 and (
                min_index is None or float(row[c]) < float(row[min_index])):
            min_index = c

    return min_index


def is_to_be_asterisked(val, threshold):
    """
    Is the value to be asterisked with the primary suppression?

    :param val:
    :param threshold:
    :return:
    """
    to_be_asterisked = False
    if 0.0 < float(val) < threshold:
        to_be_asterisked = True
    return to_be_asterisked


def row_primary_suppression(data, rows, threshold_columns_dict, debug):
    """
    Preform primary suppression for row.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: The threshold.
    :param rows:
    :param debug:
    :return: The data set after the primary suppression on rows.
    """
    if len(data[0]) <= 3:
        return data
    for r, row in enumerate(data):
        if r == len(data) - 1:
            continue
        for c, column in enumerate(row):
            threshold = get_threshold_from_dict(threshold_columns_dict, c)
            if c < len(rows) or c == len(row) - 1:
                continue
            val = row[c]
            if str(val).startswith(ASTERISK):
                continue
            if is_to_be_asterisked(val, threshold):
                row[c] = ASTERISK
                if debug:
                    row[c] += 'R(' + val + ")"

    return data


def column_primary_plain_suppression(data,
                                     threshold_columns_dict,
                                     debug):
    """
    Delete under threshold values on plain table.

    :param data:
    :param threshold_columns_dict:
    :param debug:
    :return:
    """
    if len(data) <= 3:
        return data

    for c, column in enumerate(data[0], start=0):
        threshold = get_threshold_from_dict(threshold_columns_dict, c)
        if c == len(data[0]):
            break
        for r, row in enumerate(data):
            if r == len(data) - 1:
                continue
            val = row[c]
            if str(val).startswith(ASTERISK):
                continue
            if is_to_be_asterisked(val, threshold):
                row[c] = ASTERISK
                if debug:
                    row[c] += 'C(' + str(val) + ")"
    return data



def column_primary_pivoted_suppression(data,
                                       obs_values,
                                       threshold_columns_dict,
                                       debug):
    """
    Perform primary suppression on a pivoted table.
    This support multiple observation values.

    :rtype : list of tuples.
    :param data: List of tuples containing query result set.
    :param obs_values: Observation values.
    :param threshold_columns_dict: The threshold dictionary.
    :param debug: Debug flag.
    :return: The data set after the primary suppression on columns.
    """
    if len(data) <= 3:
        return data

    for obs in obs_values:
        threshold = get_threshold_from_dict(threshold_columns_dict, obs)

        for c, column in enumerate(data[0], start=0):
            if c == len(data[0]):
                break
            for r, row in enumerate(data):
                if r == len(data) - len(obs_values) or r % len(
                        obs_values) != obs:
                    continue
                val = row[c]
                if str(val).startswith(ASTERISK):
                    continue
                if is_to_be_asterisked(val, threshold):
                    row[c] = ASTERISK
                    if debug:
                        row[c] += 'C' + str(obs) + '(' + str(val) + ")"
    return data


def row_secondary_suppression(data, rows, debug):
    """
    Perform secondary suppression for rows.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :param data: List of tuples containing query result set.
    :param rows:
    :param debug: Debug flag.
    :return: The data set after the secondary suppression on rows.
    """
    asterisked = 0
    if len(data[0]) <= 3:
        return data, asterisked

    for r, row in enumerate(data):
        asterisk = 0
        if r == len(data) - 1:
            continue
        for c, column in enumerate(row):
            val = str(row[c])
            if val.startswith(ASTERISK):
                asterisk += 1
            if c < len(rows) or c == len(row) - 1:
                continue
            if asterisk > 1:
                break
        if asterisk == 1:
            min_c = find_column_with_min_value(data, row)
            if min_c is None:
                continue
            value = row[min_c]
            row[min_c] = ASTERISK
            if debug:
                row[min_c] += ASTERISK + 'R(' + str(value) + ")"
            asterisked += 1
            if float(value) == 0.0 and asterisked == 1:
                min_c = find_column_with_min_value_exclude_zero(data,
                                                                row)
                if min_c is None:
                    continue
                value = row[min_c]
                row[min_c] = ASTERISK
                if debug:
                    row[min_c] += ASTERISK + 'R(' + str(value) + ")"
                asterisked += 1
    return data, asterisked


def column_secondary_suppression(data, rows, columns, obs_values, debug):
    """
    Perform secondary suppression for columns.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :rtype :  List of tuples.
    :param data: List of tuples containing query result set.
    :param rows: Rows where not apply the suppression.
    :param columns: Columns where not apply the suppression.
    :param obs_values: Observation values.
    :param debug: If active show debug info on asterisked cells.
    :return: The data set after the secondary suppression on columns.
    """
    asterisked = 0
    if len(data) <= 3:
        return data, asterisked

    for c, column in enumerate(data[0], start=len(rows)):
        asterisk = 0
        if c == len(data[0]):
            break

        for r, row in enumerate(data, start=len(columns)):
            val = str(row[c])
            if val.startswith("*"):
                asterisk += 1
            if asterisk > 1:
                break

        if asterisk == 1:
            min_r = find_row_with_min_value(data, 0, len(data) - 2, c)
            if min_r is None:
                continue
            value = data[min_r][c]
            data[min_r][c] = ASTERISK
            if debug:
                data[min_r][c] += ASTERISK + 'C(' + str(value) + ")"
            asterisked += 1

            if float(value) == 0.0 and asterisked == 1:
                min_r = find_row_with_min_value_exclude_zero(data,
                                                             0,
                                                             len(data) - 2,
                                                             c)
                if min_r is not None:
                    value = data[min_r][c]
                    data[min_r][c] = ASTERISK
                    if debug:
                        data[min_r][c] += ASTERISK + 'C(' + str(value) + ")"
                    asterisked += 1

    return data, asterisked


def protect_pivoted_secret_with_marginality(data,
                                            rows,
                                            obs_values,
                                            threshold_columns_dict,
                                            debug):
    """
    Protect pivoted secret with marginality.

    :param data: List of tuples containing query result set.
    :param rows:
    :param obs_values:
    :param threshold_columns_dict: Threshold column dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving statistical secret with marginality.
    """
    data = row_primary_suppression(data, rows, threshold_columns_dict, debug)

    data = column_primary_pivoted_suppression(data, obs_values,
                                              threshold_columns_dict, debug)

    return data


def protect_pivoted_table(data,
                          rows,
                          columns,
                          secret_column_dict,
                          sec_ref,
                          threshold_columns_dict,
                          constraint_cols,
                          obs_values,
                          debug):
    """
    Protect pivoted table by statistical secret.

    :param rows:
    :param columns: Columns.
    :param threshold_columns_dict: Threshold columns.
    :param constraint_cols: Column with constraints,
    :param obs_values:  Observable values.
    :param data: List of tuples containing query result set.
    :param secret_column_dict: Dictionary of secret column.
    :param sec_ref: Constraint on external table.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving the statistical secret.
    """

    if len(secret_column_dict) + len(sec_ref) + len(constraint_cols) == 0:
        return data
    elif len(secret_column_dict) + len(sec_ref) > 0:
        data = protect_pivoted_secret_with_marginality(data,
                                                       rows,
                                                       obs_values,
                                                       threshold_columns_dict,
                                                       debug)
    else:
        data = protect_pivoted_secret(data,
                                      threshold_columns_dict,
                                      rows,
                                      columns,
                                      debug)

    tot_asterisked = 1
    while tot_asterisked > 0:
        data, asterisked_r = row_secondary_suppression(data, rows, debug)
        data, asterisked_c = column_secondary_suppression(data,
                                                          rows,
                                                          columns,
                                                          obs_values,
                                                          debug)
        tot_asterisked = asterisked_c + asterisked_r

    return data


def protect_secret(data, threshold_columns_dict, debug):
    """
    Add asterisk in cells to preserve the statistical secret.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: The threshold dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The table preserving the statistical secret-
    """
    warning = None
    for row in data:
        for column in threshold_columns_dict:
            value = row[column]
            if float(value) < threshold_columns_dict[column]:
                row[column] = ASTERISK
                if debug:
                    row[column] += "(" + value + ")"

                if warning is None:
                    warning = PRESERVE_STAT_SECRET_MSG
    return data, warning


def item_constraint(constraint):
    """
    Take a constraint string and return a lazy structure
    representing the item.

    :param constraint:
    :return:
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


def apply_constraint_pivot(data,
                           data_frame,
                           pivot_cols,
                           rows,
                           col_dict,
                           constraint_cols,
                           debug):
    """
    Apply a constraint limit to the result set.

    :param data: List of tuples containing query result set.
    :param col_dict: Columns dictionary.
    :param constraint_cols: Constraints dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table applying constraints.
    """
    constraint_dict = build_constraint_dict(constraint_cols)

    for con, constraint in enumerate(constraint_dict):
        constraint_values = constraint_dict[constraint]
        table = constraint_values[0]['table']
        enum_column = constraint_values[0]['column']
        dest_table_description = get_table_schema(table)
        dest_columns = []
        for field in dest_table_description:
            dest_columns.append(field.name)

        to_be_selected_columns = []
        for col in col_dict:
            col_name = col_dict[col]['column']
            if col_name in dest_columns:
                to_be_selected_columns.append(col_name)

        query, new_header = build_constraint_query(table,
                                                   to_be_selected_columns,
                                                   enum_column,
                                                   col_dict,
                                                   constraint_values)
        st = detect_special_columns(query)
        query = build_description_query(query, st.cols, False)

        dest_data = execute_query_on_main_db(query)

        if len(rows) > 1:
            data_frame.sortlevel(inplace=True)

        for row in dest_data:
            start_col = 0
            key = []
            for c, co in enumerate(pivot_cols):
                c_name = col_dict[co]['column']
                if c_name in new_header:
                    p_col = stringify(row[c])
                    key.append(p_col)
                    start_col += 1

            try:
                if len(key) == 1:
                    column_index = data_frame.columns.get_loc(key[0])
                else:
                    column_index = data_frame.columns.get_loc(tuple(key))
            except KeyError:
                continue

            key = []
            for c, ro in enumerate(to_be_selected_columns[start_col:],
                                   start=start_col):
                p_col = stringify(row[c])
                key.append(p_col)

            try:
                row_index = data_frame.index.get_loc(tuple(key))
            except KeyError:
                continue

            start_row = row_index.start
            stop_row = row_index.stop
            sel_row = start_row
            while sel_row != stop_row:
                if sel_row % len(constraint_dict) == con:
                    constraint_val = row[len(row)-1]
                    src_row = data[sel_row]
                    if isinstance(column_index, slice):
                        start_col = column_index.start
                        stop_col = column_index.stop
                        sel_col = start_col
                        while sel_col != stop_col:
                            val = src_row[sel_col]
                            src_row[sel_col] = ASTERISK
                            if debug:
                                src_row[sel_col] += "(%s, %s" % (val,
                                                                 enum_column)
                                src_row[sel_col] += "=%s)" % constraint_val
                            sel_col += 1
                    else:
                        val = src_row[column_index]
                        src_row[column_index] = ASTERISK
                        if debug:
                            src_row[column_index] += "(%s, %s" % (val,
                                                                  enum_column)
                            src_row[column_index] += "=%s)" % constraint_val
                sel_row += 1

    return data


def data_frame_from_tuples(data_frame, data):
    """
    Create a new Pandas data_frame with data.

    :param data_frame: Pandas data frame to be clone.
    :param data: List of tuples containing query result set.
    :return: A Pandas data frame with the data_frame schema and data.
    """
    ret = pd.DataFrame.from_records([],
                                    columns=data_frame.columns,
                                    index=data_frame.index)
    for r, row in enumerate(data):
        if r > len(data_frame.index)-1:
            continue
        index = data_frame.index[r]
        for c, o in enumerate(data_frame.columns):
            value = data[r][c]
            ret.set_value(index, data_frame.columns[c], value)
    return ret


def apply_constraint_plain(data,
                           col_dict,
                           constraint_cols,
                           debug):
    """
    Apply a constraint limit to the result set on plain table.

    :param data: List of tuples containing query result set.
    :param col_dict: Columns dictionary.
    :param constraint_cols: Constraints dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The plain table applying constraints.
    """

    constraint_dict = build_constraint_dict(constraint_cols)

    for constraint in constraint_dict:
        constraint_values = constraint_dict[constraint]
        table = constraint_values[0]['table']
        enum_column = constraint_values[0]['column']
        dest_table_description = get_table_schema(table)
        dest_columns = []
        for field in dest_table_description:
            dest_columns.append(field.name)

        to_be_selected_columns = []
        for col in col_dict:
            col_name = col_dict[col]['column']
            if col_name in dest_columns:
                to_be_selected_columns.append(col_name)

        query, new_header = build_constraint_query(table,
                                                   to_be_selected_columns,
                                                   enum_column,
                                                   col_dict,
                                                   constraint_values)
        st = detect_special_columns(query)
        query = build_description_query(query, st.cols, False)

        dest_data = execute_query_on_main_db(query)

        for c, column in enumerate(data[0], start=1):
            if c == len(data[0]) - 1:
                break
            for r, row in enumerate(data):
                if r == len(data) - 1:
                    continue
                val = data[r][c]
                constraint_val = float(dest_data[r][c])
                data[r][c] = ASTERISK
                if debug:
                    data[r][c] += "(" + str(val) + "," + str(enum_column)
                    data[r][c] += "=" + str(constraint_val) + ")"

    return data


def protect_plain_table(data,
                        threshold_columns_dict,
                        debug):
    """
    Protect plain table by statistical secret.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: The threshold dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The plain table preserving the statistical secret.
    """
    threshold = get_threshold_from_dict(threshold_columns_dict)
    data = column_primary_plain_suppression(data, [0], debug)

    data, asterisked_c = column_secondary_suppression(data,
                                                      [0],
                                                      [],
                                                      debug)

    return data


def append_total_to_plain_table(data, threshold_columns_dict, constraint_cols):
    """
    Append total to the plain table.

    :param data:
    :param threshold_columns_dict:
    :param constraint_cols:
    :return:
    """
    sum_row = []
    c = 1
    sum_row.append(unicode(_("Total")))
    while c < len(data[0]):
        if c in threshold_columns_dict or c in constraint_cols:
            total = 0
            for r, row in enumerate(data):
                total += data[r][c]
            sum_row.append(total)
        else:
            sum_row.append("")
        c += 1
    data.append(sum_row)
    return data


def apply_stat_secret_plain(headers,
                            data,
                            col_dict,
                            threshold_columns_dict,
                            constraint_cols,
                            debug):
    """
    Take in input the full data set and the column descriptions
    and return the data set statistical secret free.
    This works on plain tables.

    :param data: List of tuples containing query result set.
    :param headers: Result set header.
    :param threshold_columns_dict: Threshold dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: data, headers, data_frame, warn, err.
    """
    if data is None:
        return None

    data = append_total_to_plain_table(data,
                                       threshold_columns_dict,
                                       constraint_cols)

    # Now apply constraint, primary and secondary on plain table.
    data = apply_constraint_plain(data,
                                  col_dict,
                                  constraint_cols,
                                  debug)

    data = protect_plain_table(data,
                               threshold_columns_dict,
                               debug)

    df = pd.DataFrame(data, columns=headers)

    if (df.shape[1]) == 2:
        df = drop_total_column(df)
    if len(df.index) == 2:
        df = drop_total_row(df)

    return data, headers, df


def apply_stat_secret(headers,
                      data,
                      col_dict,
                      pivot_columns,
                      secret_column_dict,
                      sec_ref,
                      threshold_columns_dict,
                      constraint_cols,
                      debug):
    """
    Take in input the full data set and the column descriptions
    and return the data set statistical secret free.

    :param data: List of tuples containing query result set.
    :param headers: Result set header.
    :param secret_column_dict: Secret column dictionary.
    :param threshold_columns_dict: Threshold dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: data, headers, data_frame, warn, err.
    """
    warn = None
    err = None

    if pivot_columns is not None and len(pivot_columns) > 0:
        pivot_cols = []
        pivot_values = []
        for pivot_col in pivot_columns:
            pivot_cols.append(headers[pivot_col])

        obs_values = list_obs_value_column_from_dict(col_dict)

        for v in obs_values:
            pivot_values.append(headers[v])

        if len(pivot_values) == 0:
            err = _("Can not pivot the table; missing numerosity!")
            return data, headers, None, warn, err

        rows = []
        for k in col_dict:
            column_name = headers[k]

            if not (column_name in pivot_cols or column_name in pivot_values):
                rows.append(headers[k])

        if len(rows) == 0:
            err = _("Can not pivot the table; missing rows!")
            return rows, headers, None, warn, err

        data, data_frame, err = pivot(data,
                                      headers,
                                      pivot_cols,
                                      rows,
                                      pivot_values)

        if len(obs_values) > 1:
            data_frame = data_frame.stack(0)
            data = get_data_from_data_frame(data_frame)

        if err:
            return rows, headers, None, warn, err

        data = apply_constraint_pivot(data,
                                      data_frame,
                                      pivot_columns,
                                      rows,
                                      col_dict,
                                      constraint_cols,
                                      debug)

        data = protect_pivoted_table(data,
                                     rows,
                                     pivot_cols,
                                     secret_column_dict,
                                     sec_ref,
                                     threshold_columns_dict,
                                     constraint_cols,
                                     obs_values,
                                     debug)

        if (data_frame.shape[1]) == 2:
            data_frame = drop_total_column(data_frame)

        if len(data_frame.index) == 2:
            data_frame = drop_total_row(data_frame)

        data_frame = data_frame_from_tuples(data_frame, data)

        return data, headers, data_frame, warn, err

    # If plain and secret does not return it.
    if len(secret_column_dict) > 0 or len(constraint_cols) > 0 or len(
            sec_ref) > 0:
        return None, headers, None, warn, err

    if len(data) > 0:
        data_frame = pd.DataFrame(data, columns=headers)
    else:
        data_frame = pd.DataFrame(columns=headers)

    return data, headers, data_frame, warn, err


def headers_and_data(query,
                     aggregation,
                     agg_filters,
                     pivot_cols,
                     debug,
                     include_descriptions):
    """
    Execute query, get headers, data, duration, error
    and filter result set to preserve the statistical secret.

    :param query: Explorer query.
    :param aggregation: If active perform an aggregation to an higher level.
    :param pivot_cols: Columns on pivot table.
    :param debug: If active show debug info on asterisked cells.
    :return: df, data, duration, warn, err.
    :param include_descriptions: Force to include description.
    """
    warn = None
    df = None
    st = detect_special_columns(query.sql)

    if len(aggregation) > 0:
        query.sql, err = build_aggregation_query(query.sql,
                                                 st.cols,
                                                 aggregation,
                                                 agg_filters,
                                                 st.threshold)

        st = detect_special_columns(query.sql)

    if include_descriptions or st.include_descriptions:
        query.sql = build_description_query(query.sql, st.cols, False)

    old_head, data, duration, err = query.headers_and_data()
    if err is None:
        if len(old_head) < 3 and len(st.secret) + len(st.constr) + len(
                st.sec_ref) == 1 and len(st.thresh) == 1:
            data, head, df = apply_stat_secret_plain(old_head,
                                                     data,
                                                     st.cols,
                                                     st.threshold,
                                                     st.constraint,
                                                     debug)

        # Check id I can give the full result set.
        elif (len(st.secret) + len(st.constraint) + len(st.secret_ref) == 0) \
                or (pivot_cols is not None and len(pivot_cols) > 0):
            data, old_head, df, warn, err = apply_stat_secret(old_head,
                                                              data,
                                                              st.cols,
                                                              pivot_cols,
                                                              st.secret,
                                                              st.secret_ref,
                                                              st.threshold,
                                                              st.constraint,
                                                              debug)
    return df, data, warn, err
