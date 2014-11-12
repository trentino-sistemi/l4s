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
from web.utils import execute_query_on_main_db, \
    build_constraint_query, \
    build_aggregation_query, \
    detect_special_columns, \
    list_obs_value_column_from_dict, \
    get_data_from_data_frame, \
    drop_total_column, \
    drop_total_row, \
    build_description_query, \
    get_data_frame_first_index,\
    get_table_metadata_value, \
    build_secondary_query, \
    has_data_frame_multi_level_columns, \
    has_data_frame_multi_level_index, \
    remove_code_from_data_frame, \
    contains_ref_period
import itertools

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

    total = unicode(_("Total")).encode('ascii')
    pivot_df.rename(columns={'All': total}, inplace=True)
    pivot_df.rename(index={'All': total}, inplace=True)
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


def row_primary_suppression(data,
                            secret_column,
                            threshold_columns_dict,
                            debug):
    """
    Preform primary suppression for row.

    :param data: List of tuples containing query result set.
    :param secret_column: Columns with secrets.
    :param threshold_columns_dict: The threshold.
    :param debug: Is to be debugged?
    :return: The data set after the primary suppression on rows.
    """
    if len(data[0]) <= 3:
        return data
    for r, row in enumerate(data):
        if r == len(data) - 1:
            continue
        for c, column in enumerate(row):
            if not c in secret_column:
                continue
            threshold = get_threshold_from_dict(threshold_columns_dict, c)
            if c == len(row):
                break
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

    :param data: Data.
    :param threshold_columns_dict: Dictionary of columns with threshold.
    :param debug: Is to be debugged?
    :return: Data.
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
                                       secret_column,
                                       threshold_columns_dict,
                                       debug):
    """
    Perform primary suppression on a pivoted table.
    This support multiple observation values.

    :rtype : list of tuples.
    :param data: List of tuples containing query result set.
    :param obs_values: Observation values.
    :param secret_column: Secret column list.
    :param threshold_columns_dict: The threshold dictionary.
    :param debug: Debug flag.
    :return: The data set after the primary suppression on columns.
    """
    if len(data) <= 3:
        return data

    for obs in obs_values:
        threshold = get_threshold_from_dict(threshold_columns_dict, obs)

        for c, column in enumerate(data[0], start=0):
            if c == len(data[0]) - 1:
                break
            for r, row in enumerate(data):
                if r > len(data) - len(obs_values):
                    continue
                if len(obs_values) > 1 and r % len(obs_values) != obs:
                    continue
                val = row[c]
                if str(val).startswith(ASTERISK):
                    continue
                if is_to_be_asterisked(val, threshold):
                    row[c] = ASTERISK
                    if debug:
                        row[c] += 'C' + str(obs) + '(' + str(val) + ")"
    return data


def row_secondary_suppression(data, debug):
    """
    Perform secondary suppression for rows.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :param data: List of tuples containing query result set.
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
            if c == len(row) - 1:
                break
            val = str(row[c])
            if val.startswith(ASTERISK):
                asterisk += 1
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


def column_secondary_suppression(data, debug):
    """
    Perform secondary suppression for columns.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :rtype :  List of tuples.
    :param data: List of tuples containing query result set.
    :param debug: If active show debug info on asterisked cells.
    :return: The data set after the secondary suppression on columns.
    """
    asterisked = 0
    if len(data) <= 3:
        return data, asterisked

    for c, column in enumerate(data[0]):
        asterisk = 0
        if c == len(data[0]):
            break

        for r, row in enumerate(data):
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


def protect_pivoted_secret(data,
                           obs_values,
                           secret_column,
                           threshold_columns_dict,
                           pivot,
                           cols,
                           debug):
    """
    Protect pivoted secret with marginality.

    :param data: List of tuples containing query result set.
    :param obs_values: Observable values.
    :param secret_column: column with secret.
    :param threshold_columns_dict: Threshold column dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving statistical secret with marginality.
    """
    #@TODO Se tempo allora no row.
    if not contains_ref_period(pivot, cols, axis=0):
        data = row_primary_suppression(data,
                                       secret_column,
                                       threshold_columns_dict,
                                       debug)
    if not contains_ref_period(pivot, cols, axis=1):
        data = column_primary_pivoted_suppression(data,
                                                  obs_values,
                                                  secret_column,
                                                  threshold_columns_dict,
                                                  debug)

    return data


def protect_pivoted_table(data,
                          secret_column_dict,
                          sec_ref,
                          threshold_columns_dict,
                          constraint_cols,
                          obs_values,
                          pivot,
                          cols,
                          debug):
    """
    Protect pivoted table by statistical secret.

    :param data: List of tuples containing query result set.
    :param secret_column_dict: Dictionary of secret column.
    :param sec_ref: Constraint on external table.
    :param threshold_columns_dict: Threshold columns.
    :param constraint_cols: Column with constraints,
    :param obs_values:  Observable values.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving the statistical secret.
    """

    if len(secret_column_dict) + len(sec_ref) + len(constraint_cols) == 0:
        return data
    else:
        data = protect_pivoted_secret(data,
                                      obs_values,
                                      secret_column_dict,
                                      threshold_columns_dict,
                                      pivot,
                                      cols,
                                      debug)

    tot_asterisked = 1
    while tot_asterisked > 0:
        asterisked_r = 0
        if not contains_ref_period(pivot, cols, axis=0):
            data, asterisked_r = row_secondary_suppression(data, debug)
        asterisked_c = 0
        if not contains_ref_period(pivot, cols, axis=1):
            data, asterisked_c = column_secondary_suppression(data, debug)
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


def apply_constraint_pivot(data,
                           data_frame,
                           pivot_cols,
                           rows,
                           col_dict,
                           constraint_cols,
                           filters,
                           debug):
    """
    Apply a constraint limit to the result set.

    :param data: List of tuples containing query result set.
    :param data_frame: pandas data frame.
    :param pivot_cols: Pivot columns.
    :param rows: Rows.
    :param col_dict: Columns dictionary.
    :param constraint_cols: Constraints dictionary.
    :param filters: Filters.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table applying constraints.
    """
    constraint_dict = build_constraint_dict(constraint_cols)

    for con, constraint in enumerate(constraint_dict):
        constraint_values = constraint_dict[constraint]
        enum_column = constraint_values[0]['column']
        query, new_header = build_constraint_query(constraint_values,
                                                   col_dict,
                                                   filters)

        if query is None:
            return data

        st = detect_special_columns(query)
        query = build_description_query(query,
                                        st.cols,
                                        pivot_cols,
                                        False,
                                        False)

        dest_data = execute_query_on_main_db(query)

        if len(rows) > 1:
            data_frame.sortlevel(inplace=True)

        for row in dest_data:
            start_col = 0
            key = []
            for c, co in enumerate(pivot_cols):
                c_name = col_dict[co]['column']
                if c_name in new_header:
                    p_col = row[c]
                    key.append(p_col)
                    start_col += 1

            try:
                if len(key) == 1:
                    column_index = data_frame.columns.get_loc(key[0])
                else:
                    column_index = data_frame.columns.get_loc(tuple(key))
            except (KeyError, TypeError):
                continue

            key = []
            for c, ro in enumerate(new_header[start_col:],
                                   start=start_col):
                if c == len(new_header) - 1:
                    continue
                p_col = row[c]
                key.append(p_col)
            try:
                row_index = data_frame.index.get_loc(tuple(key))
            except (KeyError, TypeError):
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
                                src_row[sel_col] += "(%s, " % val
                                src_row[sel_col] += "%s" % enum_column
                                src_row[sel_col] += "=%s)" % constraint_val
                            sel_col += 1
                    else:
                        val = src_row[column_index]
                        src_row[column_index] = ASTERISK
                        if debug:
                            src_row[column_index] += "(%s, " % val
                            src_row[column_index] += "%s" % enum_column
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
        enum_column = constraint_values[0]['column']
        query, new_header = build_constraint_query(constraint_values,
                                                   col_dict,
                                                   dict())

        st = detect_special_columns(query)
        query = build_description_query(query, st.cols, [], False, False)

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

    :type debug: bool
    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: The threshold dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The plain table preserving the statistical secret.
    """
    data = column_primary_plain_suppression(data,
                                            threshold_columns_dict,
                                            debug)

    data, asterisked_c = column_secondary_suppression(data,
                                                      debug)

    return data


def append_total_to_plain_table(data, threshold_columns_dict, constraint_cols):
    """
    Append total to the plain table.

    :param data: Data,
    :param threshold_columns_dict: Dictionary for columns with threshold.
    :param constraint_cols: Dictionary for primary suppression constraint.
    :return: Data.
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


def secondary_row_suppression_constraint(data,
                                         data_frame,
                                         pivot_columns,
                                         rows,
                                         col_dict,
                                         secondary,
                                         filters,
                                         debug):
    """
    Secondary suppression on row following a constraint.

    :param data: Data,
    :param data_frame: Data frame.
    :param pivot_columns: Pivot columns.
    :param rows: Rows.
    :param col_dict: Columns dictionary.
    :param secondary: Constraint for secondary suppression.
    :param filters: Filters.
    :param debug: Is to be debugged?
    :return: Data, number of asterisk.
    """
    asterisk_global_count = 0
    query, new_header = build_secondary_query(secondary,
                                              col_dict,
                                              filters)

    if query is None or not has_data_frame_multi_level_columns(data_frame):
        return data, asterisk_global_count

    st = detect_special_columns(query)
    query = build_description_query(query,
                                    st.cols,
                                    pivot_columns,
                                    False,
                                    False)
    query += "\n ORDER BY %s" % new_header[len(new_header)-1]

    dest_data = execute_query_on_main_db(query)

    if len(rows) > 1:
        data_frame.sortlevel(inplace=True)

    levels_contents = []
    for l, levels in enumerate(data_frame.columns.levels):
        if l < len(data_frame.columns.levels) - 1:
            levels_list = data_frame.columns.levels[l].tolist()
            start = 0
            end = len(data_frame.columns.levels[l])-1
            levels_contents.append(levels_list[start:end])
    col_tuples = list(itertools.product(*levels_contents))

    if has_data_frame_multi_level_index(data_frame):
        levels_contents = []
        for l, levels in enumerate(data_frame.index.levels):
            if l < len(data_frame.index.levels):
                levels_list = data_frame.index.levels[l].tolist()
                if l == 0:
                    start = 0
                    end = len(data_frame.index.levels[l])-1
                    levels_contents.append(levels_list[start:end])
                else:
                    levels_contents.append(levels_list)
        index_tuples = list(itertools.product(*levels_contents))
    else:
        index_tuples = []
        for c, col in enumerate(data_frame.index):
            index_tuples.append(col)

    for rt, row_tup in enumerate(index_tuples):
            row_index = data_frame.index.get_loc(index_tuples[rt])
            src_row = data[row_index]
            for ct, col_tup in enumerate(col_tuples):
                column_index = data_frame.columns.get_loc(col_tuples[ct])
                start_col = column_index.start
                stop_col = column_index.stop
                sel_col = start_col
                asterisk_count = 0
                while sel_col != stop_col:
                    if str(src_row[sel_col]).startswith(ASTERISK):
                        asterisk_count += 1
                    sel_col += 1
                if asterisk_count == 1:
                    #Need to asterisk an other one.
                    for d_r, target_row in enumerate(dest_data):
                        if asterisk_count >= 2:
                            break
                        col_count = 1
                        cell_col_list = []
                        for i, index in enumerate(col_tuples[ct]):
                            if dest_data[d_r][i] != col_tuples[ct][i]:
                                continue
                            cell_col_list.append(dest_data[d_r][i])
                            col_count += 1

                        if len(cell_col_list) == 0:
                            continue
                        col_tuple = tuple(cell_col_list)
                        column_index = data_frame.columns.get_loc(col_tuple)
                        start_column = column_index.start
                        stop_column = column_index.stop
                        sel_column = start_column
                        while sel_column != stop_column:
                            cell = str(src_row[sel_column])
                            if not cell.startswith(ASTERISK):
                                src_row[sel_column] = ASTERISK
                                if debug:
                                    src_row[sel_column] += ASTERISK
                                    src_row[sel_column] += 'R(' + cell + ")"
                                asterisk_count += 1
                                asterisk_global_count += asterisk_count
                                break
                            sel_column += 1

    return data, asterisk_global_count


def secondary_col_suppression_constraint(data,
                                         data_frame,
                                         pivot_columns,
                                         rows,
                                         col_dict,
                                         obs_values,
                                         secondary,
                                         filters,
                                         debug):
    """
    Performs secondary suppression on columns following the secondary metadata
    rule on table.

    :param data: Data.
    :param data_frame: Data frame.
    :param pivot_columns: Pivot columns.
    :param rows: Rows.
    :param col_dict: Columns dictionary.
    :param obs_values: Observable values.
    :param secondary: Constraint for secondary suppression.
    :param filters: Filters.
    :param debug: Is to be debugged?
    :return: data, number of asterisk.
    """
    asterisk_global_count = 0
    column_tuple_list = []
    df = data_frame.replace("\*.*", "*", regex=True)
    res = df[df == ASTERISK].count()[0:len(df.columns)-1]
    for r, re in enumerate(res):
        if re == len(obs_values):
            column_tuple = res.index[r]
            column_tuple_list.append(column_tuple)

    if len(column_tuple_list) == 0:
        return data, asterisk_global_count

    query, new_header = build_secondary_query(secondary,
                                              col_dict,
                                              filters)

    if query is None:
        return data, asterisk_global_count

    st = detect_special_columns(query)
    query = build_description_query(query,
                                    st.cols,
                                    pivot_columns,
                                    False,
                                    False)
    query += "\n ORDER BY %s" % new_header[len(new_header)-1]

    dest_data = execute_query_on_main_db(query)

    if len(rows) > 1:
        data_frame.sortlevel(inplace=True)

    start_col = 0
    for c, co in enumerate(pivot_columns):
        c_name = col_dict[co]['column']
        if c_name in new_header:
            start_col += 1

    for column_tuple in column_tuple_list:
        for row in dest_data:
            found = True
            key = []
            for c, dest_co in enumerate(row):
                if c < start_col:
                    if isinstance(column_tuple, tuple):
                        if row[c] != column_tuple[c]:
                            found = False
                            break
                    elif row[c] != column_tuple:
                        found = False
                        break
                elif c != len(new_header)-1:
                    key.append(row[c])
            if found:
                column_index = data_frame.columns.get_loc(column_tuple)
                row_index = data_frame.index.get_loc(tuple(key))
                start_row = row_index.start
                stop_row = row_index.stop
                sel_row = start_row
                asterisked = False
                while sel_row != stop_row:
                    src_row = data[sel_row]
                    cell = str(src_row[column_index])
                    if not cell.startswith(ASTERISK):
                        src_row[column_index] = ASTERISK
                        if debug:
                            src_row[column_index] += ASTERISK
                            src_row[column_index] += 'C(' + cell + ")"
                        asterisked = True
                        asterisk_global_count += 1
                    sel_row += 1
                if asterisked:
                    break

    data_frame = data_frame_from_tuples(data_frame, data)

    df = data_frame.replace("\*.*", "*", regex=True)
    res = df[df == ASTERISK].count()[0:len(df.columns)-1]
    column_tuple_list = []
    for r, re in enumerate(res):
        if re == len(obs_values):
            column_tuple = res.index[r]
            column_tuple_list.append(column_tuple)

    for column_tuple in column_tuple_list:
        column_index = data_frame.columns.get_loc(column_tuple)
        min_index = None
        for r, row in enumerate(data):
            cell = str(row[column_index])
            if not cell.startswith(ASTERISK):
                min_value = data[min_index][column_index]
                if min_index is None or row[column_index] < min_value:
                    min_index = r

        for o, obs_value in enumerate(obs_values):
            cell = data[min_index + o][column_index]
            data[min_index + o][column_index] = ASTERISK
            if debug:
                data[min_index + o][column_index] += ASTERISK
                data[min_index + o][column_index] += 'C(' + cell + ")"
                asterisk_global_count += 1

    return data, asterisk_global_count


def apply_stat_secret(headers,
                      data,
                      col_dict,
                      pivot_dict,
                      secret_column_dict,
                      sec_ref,
                      threshold_columns_dict,
                      constraint_cols,
                      filters,
                      debug):
    """
    Take in input the full data set and the column descriptions
    and return the data set statistical secret free.

    :param headers: Result set header.
    :param data: List of tuples containing query result set.
    :param col_dict: Column dictionary.
    :param pivot_dict: Pivot column.
    :param secret_column_dict: Secret column dictionary.
    :param sec_ref: Secret reference...
    :param threshold_columns_dict: Threshold dictionary.
    :param constraint_cols: Columns with constraint to
                            perform primary suppression
    :param filters: Filter used in query.
    :param debug: If active show debug info on asterisked cells.
    :return: data, headers, data_frame, warn, err.
    """
    warn = None
    err = None

    if pivot_dict is not None and len(pivot_dict) > 0:
        pivot_values = []
        pivot_cols = []
        for pivot_col in pivot_dict:
            pivot_cols.append(headers[pivot_col])

        obs_vals = list_obs_value_column_from_dict(col_dict)

        for v in obs_vals:
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

        if len(obs_vals) > 1:
            data_frame = data_frame.stack(0)
            data = get_data_from_data_frame(data_frame)

        if err:
            return rows, headers, None, warn, err

        data = apply_constraint_pivot(data,
                                      data_frame,
                                      pivot_dict,
                                      rows,
                                      col_dict,
                                      constraint_cols,
                                      filters,
                                      debug)
        sec = get_table_metadata_value(col_dict[0]['table'], 'secondary')
        if not sec is None and len(sec) > 0:
            tot_asterisked = 1
            while tot_asterisked > 0:
                data_frame = data_frame_from_tuples(data_frame, data)
                data, ast_r = secondary_row_suppression_constraint(data,
                                                                   data_frame,
                                                                   pivot_dict,
                                                                   rows,
                                                                   col_dict,
                                                                   sec[0][0],
                                                                   filters,
                                                                   debug)
                data_frame = data_frame_from_tuples(data_frame, data)

                data, ast_c = secondary_col_suppression_constraint(data,
                                                                   data_frame,
                                                                   pivot_dict,
                                                                   rows,
                                                                   col_dict,
                                                                   obs_vals,
                                                                   sec[0][0],
                                                                   filters,
                                                                   debug)
                tot_asterisked = ast_c + ast_r

        else:
            data = protect_pivoted_table(data,
                                         secret_column_dict,
                                         sec_ref,
                                         threshold_columns_dict,
                                         constraint_cols,
                                         obs_vals,
                                         pivot_dict,
                                         col_dict,
                                         debug)

        data_frame = data_frame_from_tuples(data_frame, data)

        if (data_frame.shape[1]) == 2:
            data_frame = drop_total_column(data_frame)

        index = get_data_frame_first_index(data_frame)
        if len(index) == 2:
            data_frame = drop_total_row(data_frame)

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
                     filters,
                     aggregation,
                     agg_filters,
                     pivot_cols,
                     debug,
                     include_descriptions,
                     include_code):
    """
    Execute query, get headers, data, duration, error
    and filter result set to preserve the statistical secret.

    :param query: Explorer query.
    :param filters: Filters used in the query.
    :param agg_filters: Filters used in aggregation.
    :param include_code: Include code.
    :param aggregation: If active perform an aggregation to an higher level.
    :param pivot_cols: Columns on pivot table.
    :param debug: If active show debug info on asterisked cells.
    :return: df, data, duration, warn, err.
    :param include_descriptions: Force to include description.
    :param include_descriptions: Force to include code.
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
        query.sql = build_description_query(query.sql,
                                            st.cols,
                                            pivot_cols,
                                            False,
                                            include_code)
        st = detect_special_columns(query.sql)

    old_head, data, duration, err = query.headers_and_data()

    if len(data) == 0:
        return df, data, warn, err

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
                                                              st.pivot,
                                                              st.secret,
                                                              st.secret_ref,
                                                              st.threshold,
                                                              st.constraint,
                                                              filters,
                                                              debug)
    if not include_code:
        # Fix all columns before drop the codes.
        df = remove_code_from_data_frame(df)

    if contains_ref_period(st.pivot, st.cols, axis=0):
        df = drop_total_column(df)

    if contains_ref_period(st.pivot, st.cols, axis=1):
        df = drop_total_row(df)

    return df, data, warn, err
