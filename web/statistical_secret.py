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
    drop_total_row, build_description_query

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


def get_threshold_from_dict(threshold_columns_dict):
    """
    Get threshold from dictionary.

    :param threshold_columns_dict: Threshold dictionary.
    :return: The threshold.
    """
    threshold = 3.0
    for column in threshold_columns_dict:
        threshold = threshold_columns_dict[column]
        break

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
    threshold = get_threshold_from_dict(threshold_columns_dict)
    for r, row in enumerate(data, start=len(columns)):
        if r == len(data):
            break
        for c, column in enumerate(row):
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


def find_column_with_min_value(data, rows, row):
    """
    Return the index of the column with minimum value.

    :param data: List of tuples containing query result set.
    :param row: The row where found the min value.
    :return: The column index that point to the row cell with minimum value.
    """
    min_index = None
    for c, column in enumerate(row):
        if c < len(rows) or c == len(data[0]) - 1:
            continue
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if min_index is None or float(row[c]) < float(row[min_index]):
            min_index = c

    return min_index


def find_column_with_min_value_exclude_zero(data, rows, row):
    """
    Return the index of the column with minimum value excluding zero.

    :param data: List of tuples containing query result set.
    :param row: The row where found the min value.
    :return: The column index that point to the row cell with minimum value.
    """
    min_index = None
    for c, column in enumerate(row):
        if c < len(rows) or c == len(data[0]) - 1:
            continue
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if float(row[c]) != 0.0 and (
                min_index is None or float(row[c]) < float(row[min_index])):
            min_index = c

    return min_index


def build_min_row_dict(data, rows, columns):
    """
    Build a  dictionary containing the min value for rows.

    :param data: List of tuples containing query result set.
    :return: The min row dictionary.
    """
    min_dict_row = dict()
    for c, column in enumerate(data[0]):
        if c < len(rows) or c == len(data[0]) - 1:
            continue
        index = find_row_with_min_value(data, len(columns), len(data) - 2, c)
        if index is not None:
            min_dict_row[c] = data[index][c]
    return min_dict_row


def build_min_column_dict(data, rows):
    """
    Build a  dictionary containing the min value for columns.

    :param data: List of tuples containing query result set.
    :return: The min column dictionary.
    """
    min_dict_column = dict()
    for r, row in enumerate(data):
        if r == len(data) - 1:
            continue
        index = find_column_with_min_value(data, rows, row)
        min_dict_column[r] = row[index]

    return min_dict_column


def is_to_be_asterisked(val, min_val, threshold, marginality):
    """
    Is the value to be asterisked with the primary suppression?

    :param val:
    :param min_val:
    :param threshold:
    :param marginality:
    :return:
    """
    to_be_asterisked = False
    if marginality == 0.0:
        return to_be_asterisked
    percentage = float((marginality - 1) * 3)
    if float(val) == marginality:
        to_be_asterisked = True
    if 0.0 < float(val) < threshold:
        if min_val <= percentage or min_val == marginality:
            to_be_asterisked = True
    return to_be_asterisked


def row_primary_suppression(data, rows, min_dict_row, threshold, debug):
    """
    Preform primary suppression for row.

    :param data: List of tuples containing query result set.
    :param min_dict_row: The min dictionary row.
    :param threshold: The threshold.
    :return: The data set after the primary suppression on rows.
    """
    for r, row in enumerate(data):
        if r == len(data) - 1:
            continue
        for c, column in enumerate(row):
            if c < len(rows) or c == len(row) - 1:
                continue
            marginality = float(data[r][len(row) - 1])
            min_val = float(min_dict_row[c])
            val = row[c]
            if str(val).startswith(ASTERISK):
                continue
            if is_to_be_asterisked(val, min_val, threshold, marginality):
                row[c] = ASTERISK
                if debug:
                    row[c] += 'R(' + val + ")"

    return data


def column_primary_suppression(data, rows, min_dict_column, threshold, debug):
    """
    Preform primary suppression for columns.

    :param data: List of tuples containing query result set.
    :param min_dict_column: The min dictionary column.
    :param threshold: The threshold
    :return: The data set after the primary suppression on columns.
    """
    for c, column in enumerate(data[0], start=len(rows)):
        if c == len(data[0]):
            break
        for r, row in enumerate(data):
            if r == len(data) - 1:
                continue
            marginality = float(data[len(data)-1][c])
            min_val = float(min_dict_column[r])
            val = row[c]
            if str(val).startswith(ASTERISK):
                continue
            if is_to_be_asterisked(val, min_val, threshold, marginality):
                row[c] = ASTERISK
                if debug:
                    row[c] += 'C(' + str(val) + ")"
    return data


def row_secondary_suppression(data, rows, debug):
    """
    Perform secondary suppression for rows.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :param data: List of tuples containing query result set.
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
            min_c = find_column_with_min_value(data, rows, row)
            if min_c is None:
                continue
            value = row[min_c]
            row[min_c] = ASTERISK
            if debug:
                row[min_c] += ASTERISK + 'R(' + str(value) + ")"
            asterisked += 1
            if float(value) == 0.0 and asterisked == 1:
                min_c = find_column_with_min_value_exclude_zero(data,
                                                                rows,
                                                                row)
                if min_c is None:
                    continue
                value = row[min_c]
                row[min_c] = ASTERISK
                if debug:
                    row[min_c] += ASTERISK + 'R(' + str(value) + ")"
                asterisked += 1
    return data, asterisked


def column_secondary_suppression(data, rows, columns, debug):
    """
    Perform secondary suppression for columns.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :param rows: Rows where not apply the suppression.
    :param columns: Columns where not apply the suppression.
    :param debug: If active show debug info on asterisked cells.
    :param data: List of tuples containing query result set.
    :return: The data set after the secondary suppressions on columns.
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
                                            columns,
                                            threshold_columns_dict,
                                            debug):
    """
    Protect pivoted secret with marginality.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: Threshold column dictionary.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving statistical secret with marginality.
    """
    threshold = get_threshold_from_dict(threshold_columns_dict)
    min_dict_row = build_min_row_dict(data, rows, columns)
    min_dict_column = build_min_column_dict(data, rows)
    data = row_primary_suppression(data, rows, min_dict_row, threshold, debug)

    data = column_primary_suppression(data, rows, min_dict_column, threshold,
                                      debug)

    return data


def protect_pivoted_table(data,
                          rows,
                          columns,
                          secret_column_dict,
                          sec_ref,
                          threshold_columns_dict,
                          constraint_cols,
                          debug):
    """
    Protect pivoted table by statistical secret.

    :param data: List of tuples containing query result set.
    :param secret_column_dict: Dictionary of secret column.
    :param secret_column_dict: Dictionary of secret noy on column.
    :param sec_ref: Threshold dictionary.
    :param sec_ref: Constraint on external table.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving the statistical secret.
    """

    if len(secret_column_dict) + len(sec_ref) + len(constraint_cols) == 0:
        return data
    elif len(secret_column_dict) + len(sec_ref) > 1:
        data = protect_pivoted_secret_with_marginality(data,
                                                       rows,
                                                       columns,
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
        data, asterisked_c = column_secondary_suppression(data, rows, columns,
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


def build_constraint_dict(constraint_cols):
    """
    Build a dictionary with constraint.
    :param constraint_cols: Columns containing some constraints.
    """
    constraint_dict = dict()
    for c in constraint_cols:
        value = constraint_cols[c]
        words = value.split('.')
        table_name = words[0]
        words = words[1].split('<')
        column_name = words[0]
        threshold = words[1]
        constraint_dict[c] = dict()
        constraint_dict[c]['table'] = table_name
        constraint_dict[c]['column'] = column_name
        constraint_dict[c]['threshold'] = threshold

    return constraint_dict


def apply_constraint_pivot(data,
                           headers,
                           column,
                           row, col_dict,
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

    for constraint in constraint_dict:
        constraint_value = constraint_dict[constraint]
        threshold = int(constraint_value['threshold'])
        table = constraint_value['table']
        enum_column = constraint_value['column']
        dest_table_description = get_table_schema(table)
        dest_columns = []
        for field in dest_table_description:
            dest_columns.append(field.name)

        columns = []
        for col in col_dict:
            col_name = col_dict[col]['column']
            if col_name in dest_columns:
                columns.append(col_name)

        query = build_constraint_query(table, columns, enum_column, col_dict)
        dest_data = execute_query_on_main_db(query)
        values = headers[constraint]
        dest_data, nheaders, err = pivot(dest_data,
                                         headers,
                                         column,
                                         row,
                                         values)
        if err:
            return None

        for c, column in enumerate(data[0], start=1):
            if c == len(data[0]) - 1:
                break
            for r, row in enumerate(data):
                if r == len(data) - 1:
                    continue
                val = data[r][c]
                if r in dest_data:
                    dest_row = dest_data[r]
                    constraint_val = float(dest_row[r][c])
                    if 0.0 < constraint_val < threshold:
                        data[r][c] = ASTERISK
                        if debug:
                            data[r][c] += "(%s, %s" % (val, enum_column)
                            data[r][c] += "=%s)" % constraint_val

    return data


def data_frame_from_tuples(data_frame, data, rows):
    """
    Create a new Pandas data_frame with data.

    :param data_frame: Pandas data frame to be clone.
    :param data: List of tuples containing query result set.
    :param rows: Initial rows.
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
            value = data[r][c+len(rows)]
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
        constraint_value = constraint_dict[constraint]
        threshold = int(constraint_value['threshold'])
        table = constraint_value['table']
        enum_column = constraint_value['column']
        dest_table_description = get_table_schema(table)
        dest_columns = []
        for field in dest_table_description:
            dest_columns.append(field.name)

        columns = []
        for col in col_dict:
            col_name = col_dict[col]['column']
            if col_name in dest_columns:
                columns.append(col_name)

        query = build_constraint_query(table, columns, enum_column, col_dict)
        dest_data = execute_query_on_main_db(query)

        for c, column in enumerate(data[0], start=1):
            if c == len(data[0]) - 1:
                break
            for r, row in enumerate(data):
                if r == len(data) - 1:
                    continue
                val = data[r][c]
                constraint_val = float(dest_data[r][c])
                if 0.0 < constraint_val < threshold:
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

    min_dict_column = build_min_column_dict(data, [])
    threshold = get_threshold_from_dict(threshold_columns_dict)
    data = column_primary_suppression(data, [0], min_dict_column, threshold,
                                      debug)

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

        if len(threshold_columns_dict.keys()) > 0:
            for v in threshold_columns_dict.keys():
                pivot_values.append(headers[v])
        elif len(constraint_cols) > 0:
            for v in constraint_cols.keys():
                pivot_values.append(headers[v])
        else:
            for v in list_obs_value_column_from_dict(col_dict):
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

        if err:
            return rows, headers, None, warn, err

        data = apply_constraint_pivot(data,
                                      headers,
                                      pivot_cols,
                                      rows[0],
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
                                     debug)

        if (data_frame.shape[1]) == 2:
            data_frame = drop_total_column(data_frame)

        if len(data_frame.index) == 2:
            data_frame = drop_total_row(data_frame)

        data_frame = data_frame_from_tuples(data_frame, data, rows)

        return data, headers, data_frame, warn, err

    # If plain and secret does not return it.
    if len(secret_column_dict) > 0 or len(constraint_cols) > 0 or len(
            sec_ref) > 0:
        return None, headers, None, warn, err

    if len(data) > 0:
        data_frame = pd.DataFrame(data, columns=headers)
    else:
        data_frame = pd.DataFrame(columns=headers)

    data_frame = data_frame.reset_index()

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
