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
from web.utils import execute_query_on_main_db, \
    build_constraint_query, \
    build_aggregation_query, \
    detect_special_columns, \
    list_obs_value_column_from_dict, \
    get_data_from_data_frame, \
    drop_total_column, \
    drop_total_row, \
    build_description_query, \
    get_data_frame_first_index, \
    get_table_metadata_value, \
    build_secondary_query, \
    has_data_frame_multi_level_columns, \
    has_data_frame_multi_level_index, \
    remove_code_from_data_frame, \
    contains_ref_period, \
    TOTAL, \
    get_table_schema, \
    get_column_description, \
    get_class_range, \
    add_secret_field_not_selected, \
    get_color
from web.models import ExecutedQueryLog
from utils import to_utf8
from explorer.models import Query
import itertools
import tempfile
import os
import json
import uuid
import ast
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import decimal
import random


PRESERVE_STAT_SECRET_MSG = _(
    "Some value are asterisked to preserve the statistical secret")

ASTERISK = '*'

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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
    if not cell.startswith(ASTERISK):
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

def format_value(x):

    if type(x) == decimal.Decimal:
        return round(x, 0)
    else:
        return x

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

    """
    for co, column in enumerate(data):
        print co, column
        for co2, column2 in enumerate(column):
            print co2, column2
    """

    lista = []
    contatore = 0
    #print "columns ", df.columns
    #print "index ", df.index

    for a, b in enumerate(df.columns):
        for c, d in enumerate(df.index):
            contatore += 1
            #print  b, d
            #print type(df[b][d])
            if type(df[b][d]) == decimal.Decimal:
                if not b in lista:
                    lista.append(b)
                    break
                #df[b][d] = np.float64(df[b][d])

    #print contatore
    #print lista

    df[lista] = df[lista].astype(float)

    #questa sotto arrotonda
    #df = df.applymap(format_value) #se si mescolano obs value interi e obs value con la virgola non funziona

    #print bcolors.WARNING


    #value = ['Numero indipendenti medi', 'Numero dipendenti medi', 'Numeri addetti medi', 'Numero imprese']
    #value = ['D', 'A']

    #data = [[decimal.Decimal('696.00')]]

    #print "data ", data
    #print "headers ", headers
    #print "columns ", columns
    #print "rows " , rows
    #print "value " , value
    #print df

    #print bcolors.ENDC

    #print pd.show_versions(as_json=False)

    #print df.to_csv('manuel')
    #value =  ['Numero iscritti AIRE maschi', 'Numero iscritti AIRE femmine', 'Numero iscritti AIRE totale', 'Popolazione iniziale maschi', 'Popolazione iniziale femmine', 'Popolazione iniziale totale', 'Numero nati maschi', 'Numero nati femmine', 'Numero nati totali', 'Numero morti maschi', 'Numero norti femmine', 'Numero morti totale', 'Numero iscritti da altri comuni maschi', 'Numerto iscritti da altri comuni femmine', 'Numero iscritti da altri comuni totale', "Numero iscritti dall'estero maschi", "Numero iscritti dall'estero femmine", "Numero iscritti dall'estero totale", 'Numero iscritti non altrove classificabili maschi', 'Numero iscritti non altrove classificabili femmine', 'Numero iscritti non altrove classificabili totale', 'Inumero icritti totale maschi', 'Numero iscritti totale femmine', 'Numero Iscritti totale', 'Numero cancellati per altri comuni maschi', 'Numero ancellati per altri comuni femmine', 'Numero cancellati per altri comuni totale', "Numero cancellati per l'estero maschi", "Numero cancellati per l'estero femmine", "Numero cancellati per l'estero totale", 'Numero cancellati per acquisizione cittadinanza italiana', 'Numero cancellati per acquisizione cittadinanza italiana', 'Numero cancellati per acquisizione cittadinanza italiana', 'Numero cancellati per irreperibilit\xc3\xa0 maschi', 'Numero cancellati per irreperibilit\xc3\xa0 femmine', 'Numero cancellati per irreperibilit\xc3\xa0 totale', 'Numero cancellati non altrove classificabili maschi', 'Numero cancellati non altrove classificabili femmine', 'Numero cancellati non altrove classificabili totale', 'Numero cancellati maschi', 'Numero cancellati femmine', 'Numero cancellati', 'Popolazione finale maschi', 'Popolazione finale femmine', 'Popolazione finale totale', 'Numero minorenni registrati maschi', 'Numero minorenni registrati femmine', 'Numero minorenni registrati totale', 'Numero stranieri maschi', 'Numero stranieri femmine', 'Numero stranieri totale', 'Numero stranieri nati in Italia maschi', 'Numero stranieri nati in Italia femmine', 'Numero stranieri nati in Italia totale', 'Numero famiglie con almeno uno straniero', 'Numero famiglie con intestatario straniero']

    #value = ['Numero cancellati per acquisizione cittadinanza italiana']
    #value = ['Numero iscritti AIRE maschi', 'Numero iscritti AIRE femmine', 'Numero iscritti AIRE totale', 'Popolazione iniziale maschi', 'Popolazione iniziale femmine', 'Popolazione iniziale totale', 'Numero nati maschi', 'Numero nati femmine', 'Numero nati totali', 'Numero morti maschi', 'Numero norti femmine', 'Numero morti totale', 'Numero iscritti da altri comuni maschi', 'Numerto iscritti da altri comuni femmine', 'Numero iscritti da altri comuni totale', "Numero iscritti dall'estero maschi", "Numero iscritti dall'estero femmine", "Numero iscritti dall'estero totale", 'Numero iscritti non altrove classificabili maschi', 'Numero iscritti non altrove classificabili femmine', 'Numero iscritti non altrove classificabili totale', 'Inumero icritti totale maschi', 'Numero iscritti totale femmine', 'Numero Iscritti totale', 'Numero cancellati per altri comuni maschi', 'Numero ancellati per altri comuni femmine', 'Numero cancellati per altri comuni totale', "Numero cancellati per l'estero maschi", "Numero cancellati per l'estero femmine", "Numero cancellati per l'estero totale"]

    #'Numero cancellati per irreperibilit\xc3\xa0 maschi', 'Numero cancellati per irreperibilit\xc3\xa0 femmine', 'Numero cancellati per irreperibilit\xc3\xa0 totale',


    """
    print "columns " , columns
    print "rows ", rows
    print "value ", value
    """

    try:
        pivot_df = df.pivot_table(columns=columns,
                                  index=rows,
                                  values=value,
                                  margins=True,
                                  aggfunc=np.sum)
    except Exception, e:
        #print "errore ", e
        error = _("I can not pivot the table")
        e_value = str(e)
        if e_value == "All objects passed were None":
            e_value = _("All objects passed were None")

        error = "%s: %s" % (unicode(error), unicode(e_value))
        return None, None, error

    """
    print bcolors.OKGREEN
    print pivot_df
    print bcolors.ENDC
    """

    pivot_df = pivot_df.applymap(
        lambda a: str(a).replace(".0", "", 1).replace("nan", "0"))

    total = unicode(_("Total")).encode('ascii')
    pivot_df.rename(columns={'All': total}, inplace=True)
    pivot_df.rename(index={'All': total}, inplace=True)
    data = get_data_from_data_frame(pivot_df)

    #print bcolors.OKBLUE
    #print pivot_df
    #print bcolors.ENDC

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


def find_column_with_min_value(row, start_index, end_index):
    """
    Return the index of the column with minimum value
    between start and end index.

    :param row: The row where found the min value.
    :param start_index: Start index.
    :param end_index: End index.
    :return: The column index that point to the row cell with minimum value.
    """
    min_index = None

    #print "inizio ---------------"
    #print start_index
    #print end_index

    for co, column in enumerate(row[start_index:]):

        #print column

        c = start_index + co
        if c == len(
                row) - 1 or c == end_index:  # non capisco ... bastava mettere end_index nell'enumerate
            break
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if min_index is None or float(row[c]) < float(row[min_index]):
            min_index = c

    #print "fine ---------------"

    return min_index


def find_column_with_min_value_exclude_zero(row, start_index, end_index):
    """
    Return the index of the column with minimum value excluding zero.

    :param row: The row where found the min value.
    :param start_index: Start index.
    :param end_index: End index.
    """
    min_index = None
    for co, column in enumerate(row[start_index:]):
        c = start_index + co
        if c == len(row) - 1 or c == end_index:
            break
        val = row[c]
        if str(val).startswith(ASTERISK):
            continue
        if float(row[c]) != 0.0 and (
                        min_index is None or float(row[c]) < float(
                        row[min_index])):
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
                            threshold_columns_dict,
                            obs_values,
                            debug):
    """
    Preform primary suppression for row.

    :param data: List of tuples containing query result set.
    :param threshold_columns_dict: The threshold.
    :param debug: Is to be debugged?
    :return: The data set after the primary suppression on rows.
    """
    for r, row in enumerate(data):
        if r == len(data) - len(obs_values):  #per saltare la riga dei totali
            continue
        for c, column in enumerate(row):
            threshold = get_threshold_from_dict(threshold_columns_dict,
                                                c)  #estrare la soglia se c'e' altrimenti usa quella di default che e' 3
            if c == len(row) - 1:  #per saltare la colonna dei totali
                break
            val = row[c]  #usare column ??????????
            if str(val).startswith(
                    ASTERISK):  #sara' mai vero ? e' la prima procedura che cicla su data e quindi asterischi non dovrebbero essercene
                continue
            if is_to_be_asterisked(val, threshold):
                row[c] = ASTERISK
                if debug:
                    row[c] += 'P(' + val + ")"

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

    for o, obs in enumerate(obs_values):  #questo e' corretto non quello che c'e' sulla riga
        threshold = get_threshold_from_dict(threshold_columns_dict, obs)

        for c, column in enumerate(data[0],
                                   start=0):  #e' un modo buffo per ciclare sulle colonne
            if c == len(data[0]) - 1:  #per saltare i totali di riga
                break
            for r, row in enumerate(data):
                if r > len(data) - len(
                        obs_values) - 1:  #per saltare i totali di colonna
                    break
                if len(obs_values) > 1 and r % len(
                        obs_values) != obs:  #per piu obs value ... ma non l'ho capita del tutto
                    continue
                val = row[c]
                if str(val).startswith(
                        ASTERISK):  # se e' gia' stato asteriscato da quella per riga
                    continue
                if is_to_be_asterisked(val, threshold):
                    row[c] = ASTERISK
                    if debug:
                        row[c] += 'P' + '(' + str(val) + ")"

                        #non va bene .... va fatto all'interno dello slice
                        #if val == data[len(data)-len(obs_values)+o][c]: # se il valore e' uguale al totale di colonna ???? serve ancora questo pezzo di codice ?????? e se e' qui perche non sulla riga ?
                        #    row[c] = ASTERISK
                        #    if debug:
                        #        row[c] += 'P' + '(TOT,' + str(val) + ")"

    return data


def estrai_tuple_per_colonne(data_frame, slices, obs_vals):

    #print has_data_frame_multi_level_columns(data_frame)

    if slices:
        if has_data_frame_multi_level_columns(data_frame):

            col_tuples = data_frame.columns
            #print "col_tuples prima " , col_tuples

            slice_da_preservare_originale, slice_da_preservare = estrai_slice_da_preservare(False,data_frame.columns, obs_vals)  #procedura per estrarre lo slice piu dettagliato

            #print "slice_da_preservare ", slice_da_preservare
            #print "data_frame.columns.levels ", data_frame.columns.levels

            i = 0
            for l, levels in enumerate(data_frame.columns.levels):  # droppo i livelli maggiori dello slice da preservare

                #print l, levels

                if i >= slice_da_preservare:
                    col_tuples = col_tuples.droplevel(i)
                else:
                    i += 1

            #print "col_tuples dopo " , col_tuples

            if type(col_tuples) <> pd.MultiIndex:  # se quello che e' rimasto e' diventato NON multilevel
                #print "lllllllll"
                # va bene che lo slice_da_preservare_originale rimanga invariato
                lista_appoggio = []
                appoggio = col_tuples.tolist()
                lista_appoggio.append(appoggio)
                col_tuples = list(itertools.product(*lista_appoggio))  #fa' il prodotto cartesiano di lista_appoggio

            #print "col_tuples " , col_tuples

        else:
            slice_da_preservare_originale = 0
            col_tuples = []
            for c, col in enumerate(data_frame.columns):
                col_tuples.append(col)

        col_tuples = list(set(col_tuples))  #rende uniche le tuple

    else:
        slice_da_preservare_originale = 0
        col_tuples = []
        for c, col in enumerate(data_frame.columns):
            col_tuples.append(col)

    return slice_da_preservare_originale, col_tuples


def estrai_tuple_per_righe(data_frame, slices, obs_vals):

    if slices:
        if has_data_frame_multi_level_index(data_frame):  #questo a differenza delle colomme potrebbe non essere multiindice es: se coninvolgo un campo che non ha decodifica

            index_tuples = data_frame.index

            slice_da_preservare_originale, slice_da_preservare = estrai_slice_da_preservare(True,data_frame.index, obs_vals)  #procedura per estrarre lo slice piu dettagliato

            #print "slice_da_preservare_originale ", slice_da_preservare_originale
            #print "slice_da_preservare ", slice_da_preservare

            i = 0
            for l, levels in enumerate(data_frame.index.levels):  # droppo i livelli maggiori dello slice da preservare
                if i >= slice_da_preservare:
                    index_tuples = index_tuples.droplevel(i)
                else:
                    i += 1

            if type(index_tuples) <> pd.MultiIndex:  # se quello che e' rimasto e' diventato NON multilevel
                # va bene che lo slice_da_preservare_originale rimanga invariato
                lista_appoggio = []
                appoggio = index_tuples.tolist()
                lista_appoggio.append(appoggio)
                index_tuples = list(itertools.product(*lista_appoggio))  #fa' il prodotto cartesiano di lista_appoggio

                #print index_tuples

        else:
            slice_da_preservare_originale = 0
            index_tuples = []
            for c, index in enumerate(data_frame.index):
                index_tuples.append(index)

        index_tuples = list(set(index_tuples))  #rende uniche le tuple

    else:
        slice_da_preservare_originale = 0
        index_tuples = []
        for c, index in enumerate(data_frame.index):
            index_tuples.append(index)

    return slice_da_preservare_originale, index_tuples


def row_secondary_suppression(data,
                              data_frame,
                              rows,
                              debug,
                              obs_values):
    """
    Secondary suppression.

    :param data:
    :param data_frame:
    :param rows:
    :param debug:
    :return:
    """

    #arrivati qui nel data_frame ho ancora i dati vergini senza asterischi della primaria, solo in data ho gli asterischi della primaria

    asterisk_global_count = 0

    #print datetime.now().strftime("%H:%M:%S.%f")

    data_frame_appoggio_colonne = data_frame.sortlevel(axis=1)  #riordina le colonne ....... menatona .... se c'e' il totale e campi stringa (es. comuni) su quella colonna sclera ... allora riordino ....

    data_frame_appoggio_colonne.drop((','.join(data_frame_appoggio_colonne.columns.levels[0]), TOTAL), axis=1, inplace=True)  # e poi tolgo la label TOTALE sulle colonne

    slice_da_preservare_colonne, col_tuples = estrai_tuple_per_colonne(data_frame_appoggio_colonne, True, obs_values)  #True perche devo tutelare gli slices di colonna

    if has_data_frame_multi_level_index(data_frame):  #riordina il data_freame per le righe... l'if per le colonne non serve per via che sono sempre multilevel
        data_frame_appoggio_righe = data_frame.sortlevel(axis=0)
    else:
        data_frame_appoggio_righe = data_frame.sort_index(axis=0)

    data_frame_appoggio_righe.drop(TOTAL, axis=0, inplace=True)  # e poi tolgo la label TOTALE sulle righe

    slice_da_preservare_righe, index_tuples = estrai_tuple_per_righe(data_frame_appoggio_righe, False, obs_values)  #false percghe mi servono tutte le righe e non mi servono gli slices di riga

    for rt, row_tup in enumerate(index_tuples):

        try:
            row_index = data_frame_appoggio_righe.index.get_loc(
                index_tuples[rt])
        except (KeyError, TypeError):
            continue

        src_row = data[
            row_index]  #lista delle righe con la sopressione primaria

        #print row_tup
        #print src_row

        for ct, col_tup in enumerate(col_tuples):

            #print col_tup

            try:
                column_index = data_frame_appoggio_colonne.columns.get_loc(
                    col_tuples[ct])
            except (KeyError, TypeError):
                continue

            #print column_index

            if not isinstance(column_index,
                              slice):  #non entra mai .... sulle colonne uno slice c'e' sempre ?????
                start_col = 0
                stop_col = len(src_row) - 1
            else:
                start_col = column_index.start
                stop_col = column_index.stop

            #print start_col
            #print stop_col

            sel_col = start_col
            asterisk_count = 0

            totale_slice = 0

            while sel_col != stop_col:  #scorro per estrarre il numero degli asterischi in quel slice

                #totale_slice += float(data_frame.iloc[row_index][sel_col]) #questo rallenta l'elaborazione ... il problema e' che in data ho gia' il dato con l'asterisco

                if str(src_row[sel_col]).startswith(ASTERISK):
                    asterisk_count += 1
                sel_col += 1

            sel_col = start_col

            if float(totale_slice) <> 0.0:

                while sel_col != stop_col:  #riscorro lo slice per asteiscare valore che coincidono col totale dello slice

                    if not str(src_row[sel_col]).startswith(ASTERISK):

                        if float(src_row[sel_col]) == totale_slice:
                            #print "bingo"
                            value = src_row[sel_col]
                            src_row[sel_col] = ASTERISK

                            if debug:
                                src_row[sel_col] += ASTERISK
                                src_row[sel_col] += 'P(' + str(
                                    value) + ' - TOT ' + str(
                                    totale_slice) + ")"

                            asterisk_count += 1
                    sel_col += 1

            if asterisk_count == 1:  # se sulla riga c'e' un asterisco solo .... ne asterisco un altro

                sel_column = find_column_with_min_value(src_row.tolist(),
                                                        start_col,
                                                        stop_col)

                if not sel_column is None:

                    value = src_row[sel_column]
                    src_row[sel_column] = ASTERISK

                    if debug:
                        src_row[sel_column] += ASTERISK
                        src_row[sel_column] += "R(%s)" % str(value)

                    asterisk_count += 1
                    asterisk_global_count += asterisk_count

                    if float(
                            value) == 0.0:  # se ho asteriscato uno zero ne asterisco un altro

                        sel_column = find_column_with_min_value_exclude_zero(
                            src_row.tolist(),
                            start_col,
                            stop_col)

                        if not sel_column is None:

                            value = src_row[sel_column]

                            src_row[sel_column] = ASTERISK

                            if debug:
                                src_row[sel_column] += ASTERISK
                                src_row[sel_column] += "R(%s)" % str(value)

                            asterisk_count += 1
                            asterisk_global_count += asterisk_count

    return data, asterisk_global_count


def estrai_slice_da_preservare(righe, livello_in, obs_vals):

    lista = []

    slice_da_preservare = -1

    for l, livello in enumerate(livello_in.levels):

        #print l
        #print livello

        appoggio = livello.tolist()

        lista.append(appoggio)

        #print lista

        lista_appoggio = list(itertools.product(*lista))

        is_slice = False

        #print lista_appoggio

        for ct, col_tup in enumerate(lista_appoggio):

            #print ct
            #print col_tup

            try:

                col_index = livello_in.get_loc(col_tup)  #slice(0, 141, None)

                if isinstance(col_index, slice):
                    is_slice = True

                    #print col_tup
            except (KeyError, TypeError):
                continue

        if is_slice == True:
            #    print str(l) + " e' uno slice "

            #    print col_index.start
            #    print col_index.stop

            if l > slice_da_preservare:
                slice_da_preservare = l

                #else:
                #    print str(l) + " non e' uno slice "

    #if slice_da_preservare > 0: da capire se farlo o meno ... per le colonne non serve
    #    slice_da_preservare -= 1

    #print type(livello_in)
    #print "------------------------------------------------------------------------------------------------slice da preservare ", slice_da_preservare
    #print "livello_in.levels " , len(livello_in.levels)

    #print bcolors.FAIL
    #print "livello_in.levels ", livello_in.levels
    #print "slice_da_preservareeeeeeeeeeeeeeeeee  " , slice_da_preservare
    #print bcolors.ENDC

    if righe:
        if len(obs_vals) == 1:

            slice_da_preservare_originale = slice_da_preservare

            if slice_da_preservare == 0:
                slice_da_preservare = 2  #2 perche qui arrivo solo se e' multilevel ..se e' zero vuol dire che devo tutelare tuttle le righe
        else:

            if slice_da_preservare <= 1:
                slice_da_preservare = len(livello_in.levels)  #perche in riga ho gli obs values
                slice_da_preservare_originale = 0
            else:
                slice_da_preservare_originale = slice_da_preservare

    else:

        if len(obs_vals) == 1:

            slice_da_preservare_originale = slice_da_preservare

            if slice_da_preservare == 0:
                slice_da_preservare = 1
        else:

            slice_da_preservare_originale = slice_da_preservare

            if slice_da_preservare == 0:
                slice_da_preservare = len(livello_in.levels) - 1   # era 2 perche qui arrivo solo se e' multilevel ..se e' zero vuol dire che devo tutelare tuttle le righe

    #print "slice da preservare " + str(slice_da_preservare)
    #print lista_start

    return slice_da_preservare_originale, slice_da_preservare


def column_secondary_suppression(data, data_frame, obs_values, debug):
    """
    Perform secondary suppression for columns.
    The function asterisk at least 2 value
    if is asterisked a 0 then is asterisked an other one.

    :rtype :  List of tuples.
    :param data: List of tuples containing query result set.
    :param obs_values: Observable values.
    :param debug: If active show debug info on asterisked cells.
    :return: The data set after the secondary suppression on columns.
    """

    #per slice multilivello proteggendo lo slice piu capillare proteggi per ereditarieta' anche gli slice superiori

    asterisk_global_count = 0

    data_frame_appoggio_colonne = data_frame.sortlevel(axis=1)  #riordina le colonne ....... menatona .... se c'e' il totale e campi stringa (es. comuni) su quella colonna sclera ... allora riordino ....
    data_frame_appoggio_colonne.drop((','.join(data_frame_appoggio_colonne.columns.levels[0]), TOTAL), axis=1, inplace=True)  # e poi tolgo la label TOTALE sulle colonne

    slice_da_preservare_colonne, col_tuples = estrai_tuple_per_colonne(data_frame_appoggio_colonne, False, obs_values)  #false perche mi servono tutte le colonne

    if has_data_frame_multi_level_index(data_frame):  #riordina il data_freame per le righe... l'if per le colonne non serve per via che sono sempre multilevel
        data_frame_appoggio_righe = data_frame.sortlevel(axis=0)
    else:
        data_frame_appoggio_righe = data_frame.sort_index(axis=0)

    data_frame_appoggio_righe.drop(TOTAL, axis=0,inplace=True)  # e poi tolgo la label TOTALE sulle righe

    slice_da_preservare_righe, index_tuples = estrai_tuple_per_righe(data_frame_appoggio_righe, True, obs_values)  #true perche mi servono gli slices .... devo tutelare gli slices di riga

    for ct, col_tup in enumerate(col_tuples):

        asterisk_count = 0

        try:
            column_index = data_frame_appoggio_colonne.columns.get_loc(col_tup)
        except (KeyError, TypeError):
            continue

        for rt, row_tup in enumerate(index_tuples):

            #print row_tup

            try:
                row_index = data_frame_appoggio_righe.index.get_loc(row_tup)
            except (KeyError, TypeError):
                continue

            #print "row_index " , row_index

            if type(data_frame_appoggio_righe.index) <> pd.MultiIndex:  #un field solo senza codice
                start_row = 0
                stop_row = len(index_tuples)
            else:
                if isinstance(row_index, slice):  #piu fields
                    start_row = row_index.start
                    stop_row = row_index.stop
                else:  #un field solo CON codice
                    start_row = 0
                    stop_row = len(index_tuples)

            #print "start_row ", start_row
            #print "stop_row " , stop_row

            asterisk_count = 0

            sel_row = start_row

            totale_slice = 0

            while sel_row != stop_row:  #scorro per estrarre il numero degli asterischi in quel slice


                #totale_slice += float(data_frame.iloc[sel_row][column_index]) #questo rallenta l'elaborazione ... il problema e' che in data ho gia' il dato con l'asterisco

                src_row = data[sel_row]

                #print sel_row , " " , src_row

                if str(src_row[column_index]).startswith(ASTERISK):
                    asterisk_count += 1
                sel_row += 1

            sel_row = start_row

            if float(totale_slice) <> 0.0:

                while sel_row != stop_row:  #riscorro lo slice per asteiscare valore che coincidono col totale dello slice

                    src_row = data[sel_row]

                    if not str(src_row[column_index]).startswith(ASTERISK):

                        if float(src_row[column_index]) == totale_slice:
                            #print "bingo"
                            value = src_row[column_index]
                            src_row[column_index] = ASTERISK

                            if debug:
                                src_row[column_index] += ASTERISK
                                src_row[column_index] += 'P(' + str(
                                    value) + ' - TOT ' + str(
                                    totale_slice) + ")"

                            asterisk_count += 1
                    sel_row += 1

            if asterisk_count == 1:

                min_row_index = find_row_with_min_value(data,
                                                        start_row,
                                                        stop_row - 1,
                                                        column_index)

                if not min_row_index is None:

                    src_row = data[min_row_index]
                    value = src_row[column_index]
                    src_row[column_index] = ASTERISK
                    if debug:
                        src_row[column_index] += ASTERISK
                        src_row[column_index] += "C(%s)" % str(
                            value)  # + ' ' + str(row_index) + ' ' + str(column_index) + ' ' + str(min_row_index)
                    asterisk_count += 1
                    asterisk_global_count += asterisk_count

                    if float(value) == 0.0:

                        min_row_index = find_row_with_min_value_exclude_zero(
                            data,
                            start_row,
                            stop_row - 1,
                            column_index)

                        if not min_row_index is None:
                            src_row = data[min_row_index]
                            value = src_row[column_index]
                            src_row[column_index] = ASTERISK
                            if debug:
                                src_row[column_index] += ASTERISK
                                src_row[column_index] += "C(%s)" % str(
                                    value)  # + ' ' + str(row_index) + ' ' + str(column_index)
                            asterisk_count += 1
                            asterisk_global_count += asterisk_count

    #print asterisk_global_count

    return data, asterisk_global_count  #perche ritorna 0 e non asterisk_global_count ?????


def protect_pivoted_secret(data,
                           obs_values,
                           threshold_columns_dict,
                           pivot_c,
                           cols,
                           debug):
    """
    Protect pivoted secret with marginality.

    :param data: List of tuples containing query result set.
    :param obs_values: Observable values.
    :param threshold_columns_dict: Threshold column dictionary.
    :param pivot_c:
    :param cols:
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving statistical secret with marginality.
    """
    if not contains_ref_period(pivot_c, cols, axis=0):
        data = row_primary_suppression(data,
                                       threshold_columns_dict,
                                       obs_values,
                                       debug)

    if not contains_ref_period(pivot_c, cols, axis=1):
        data = column_primary_pivoted_suppression(data,
                                                  obs_values,
                                                  threshold_columns_dict,
                                                  debug)

    return data


def protect_pivoted_table(data,
                          data_frame,
                          secret_column_dict,
                          sec_ref,
                          threshold_columns_dict,
                          constraint_cols,
                          obs_values,
                          pivot_c,
                          cols,
                          rows,
                          debug):
    """
    Protect pivoted table by statistical secret.

    :param data: List of tuples containing query result set.
    :param secret_column_dict: Dictionary of secret column.
    :param sec_ref: Constraint on external table.
    :param threshold_columns_dict: Threshold columns.
    :param constraint_cols: Column with constraints,
    :param obs_values:  Observable values.
    :param pivot_c: Pivot columns.
    :param cols: Columns.
    :param debug: If active show debug info on asterisked cells.
    :return: The pivoted table preserving the statistical secret.
    """

    #print "fino quiiiiiiiiiiiiiiiiiiii"

    #print "secret_column_dict ", secret_column_dict
    #print "sec_ref ", sec_ref
    #print "constraint_cols ", constraint_cols

    if len(secret_column_dict) + len(sec_ref) + len(constraint_cols) == 0:
        #print "A"
        return data
    else:
        #print "B"
        data = protect_pivoted_secret(data,
                                      obs_values,
                                      threshold_columns_dict,
                                      pivot_c,
                                      cols,
                                      debug)
    tot_asterisked = 1

    #print datetime.now().strftime("%H:%M:%S.%f")

    # si potrebbe pensare di fare prima la sopressione della dimensione con minor numerosita'
    # (es, poche righe ... faccio quella di colonna per primo)
    # poche colonne ..... faccio prima quella di riga
    # su dataset ristretti mette meno asterischi
    # su dataset grandi non cambia nulla


    while tot_asterisked > 0:
        data, asterisked_r = row_secondary_suppression(data,
                                                       data_frame,
                                                       rows,
                                                       debug,
                                                       obs_values)

        data, asterisked_c = column_secondary_suppression(data,
                                                          data_frame,
                                                          obs_values,
                                                          debug)

        #print "asterisked_c ", asterisked_c, " asterisked_r ", asterisked_r

        tot_asterisked = asterisked_c + asterisked_r

        #tot_asterisked = asterisked_c

        #print datetime.now().strftime("%H:%M:%S.%f")

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
                           aggregation,
                           debug,
                           obs_vals,
                           include_code,
                           old_cols,
                           range):
    """
    Apply a constraint limit to the result set.

    Nel caso del turismo fa la primaria contemplando il numero di strutture collegate

    :param data: List of tuples containing query result set.
    :param data_frame: pandas data frame.
    :param pivot_cols: Pivot columns.
    :param rows: Rows.
    :param col_dict: Columns dictionary.
    :param constraint_cols: Constraints dictionary.
    :param filters: Filters.
    :param debug: If active show debug info on asterisked cells.
    :param aggregation: Aggregation
    :param obs_vals: Observation value
    :return: The pivoted table applying constraints.
    """

    """
    print "pivot_cols " , pivot_cols
    print "rows " , rows
    print "col_dict " , col_dict
    print "constraint_cols " ,constraint_cols
    print "filters " , filters
    print "aggregation " , aggregation
    print "debug " , debug
    print "obs_vals " , obs_vals
    print "include_code " , include_code
    """

    constraint_dict = build_constraint_dict(constraint_cols)

    """
    print "constraint_dict ", constraint_dict
    print len(constraint_dict)

    print bcolors.OKBLUE
    print "data_frame "
    print data_frame.columns
    print data_frame.index
    print type(data_frame.columns)
    print type(data_frame.index)
    """

    if (len(constraint_dict) > 0):

        #se ce' un solo obsvalue finisce in colonna, se ce ne sono di piu finiscono in riga

        if has_data_frame_multi_level_columns(data_frame):  #riordina le colonne .......
            data_frame_appoggio_colonne = data_frame.sortlevel(axis=1)
        else:
            data_frame_appoggio_colonne = data_frame.sort_index(axis=1)

        """
        print bcolors.OKGREEN
        print "data_frame_appoggio_colonne.columns "
        print data_frame_appoggio_colonne.columns
        """

        if len(obs_vals) == 1:  #se ce' un solo obsvalue finisce in colonna .... forse e' generalizzabile anche per un caso che non sia il turismo
            data_frame_appoggio_colonne.drop((','.join(data_frame_appoggio_colonne.columns.levels[0]), TOTAL),axis=1, inplace=True)  # e poi tolgo la label TOTALE sulle colonne
        else:
            data_frame_appoggio_colonne.drop(TOTAL, axis=1,inplace=True)  # e poi tolgo la label TOTALE sulle colonne

        """
        print bcolors.HEADER
        print "data_frame_appoggio_colonne.columns "
        print data_frame_appoggio_colonne.columns
        """

        if has_data_frame_multi_level_index(data_frame):  #riordina il data_freame per le righe...
            data_frame_appoggio_righe = data_frame.sortlevel(axis=0)
        else:
            data_frame_appoggio_righe = data_frame.sort_index(axis=0)

        """
        print bcolors.WARNING
        print "data_frame_appoggio_righe.index "
        print data_frame_appoggio_colonne.index
        """

        data_frame_appoggio_righe.drop(TOTAL, axis=0,inplace=True)  # e poi tolgo la label TOTALE sulle righe

        """
        print bcolors.FAIL
        print "data_frame_appoggio_righe.index "
        print data_frame_appoggio_righe.index
        """

    #print bcolors.BOLD

    for con, constraint in enumerate(constraint_dict):

        constraint_values = constraint_dict[constraint]

        enum_column = constraint_values[0]['column']
        table = constraint_values[0]['table']
        alias = get_column_description(table, enum_column)

        query, new_header = build_constraint_query(constraint_values,
                                                   col_dict,
                                                   filters,
                                                   aggregation,
                                                   old_cols)


        #print "1 --------------------------------------------------------------------------"

        #print query

        if query is None:
            return data

        st = detect_special_columns(query)

        #print "-------------------------------------------------"
        #stampa_symtobltabel(st)
        #print "+++++++++++++++++++++++++++++++++++++++++++++++++"


        if len(aggregation) > 0:

            #torno agli elementi originali perche qui le colonne sono gia' raggruppate
            elemento = dict()
            elemento[0] = enum_column

            dest_table_description = get_table_schema(table)

            dest_columns = [field.name for field in dest_table_description]

            cols = dict()

            cols[0] = dict()
            cols[0]['table'] = table
            cols[0]['column'] = enum_column

            indice = 1

            for col in old_cols:

                col_name = old_cols[col]['column']

                if col_name in dest_columns:
                    cols[indice] = dict()
                    cols[indice]['table'] = table
                    cols[indice]['column'] = old_cols[col]['column']
                    indice += 1

            query, err = build_aggregation_query(query,
                                                 cols,
                                                 aggregation,
                                                 filters,
                                                 elemento,
                                                 constraint_values)
            st = detect_special_columns(query)

        #print "2 --------------------------------------------------------------------------"
        #print query

        #print "3 --------------------------------------------------------------------------"

        query, new_header = build_description_query(query,
                                                    st.cols,
                                                    pivot_cols,
                                                    False,
                                                    include_code)


        """
        print "4 --------------------------------------------------------------------------"
        print query
        print new_header
        """

        dest_data = execute_query_on_main_db(query)

        for row in dest_data:  #cicla sulla query con i dati delle strutture collegate

            #print row

            key_colonna = []
            for cn, column_name in enumerate(data_frame_appoggio_colonne.columns.names):
                if column_name in new_header:
                    p_col = to_utf8(row[new_header.index(column_name)])
                    appoggio = []
                    appoggio.append(p_col)
                    key_colonna.append(tuple(appoggio))
                else:
                    key_colonna.append(tuple(data_frame_appoggio_colonne.columns.levels[cn]))

            col_tuples = list(itertools.product(*key_colonna))

            #print "col_tuples " , col_tuples

            key_riga = []
            for cn, row_name in enumerate(data_frame_appoggio_righe.index.names):
                if row_name in new_header:
                    p_col = to_utf8(row[new_header.index(row_name)])
                    appoggio = []
                    appoggio.append(p_col)
                    key_riga.append(tuple(appoggio))
                else:
                    key_riga.append(
                        tuple(data_frame_appoggio_righe.index.levels[cn]))

            index_tuples = list(itertools.product(*key_riga))

            #print "index_tuples " , index_tuples

            for cn, column_name in enumerate(col_tuples):

                try:
                    if len(column_name) == 1:
                        column_index = data_frame_appoggio_colonne.columns.get_loc(column_name[0])
                    else:
                        column_index = data_frame_appoggio_colonne.columns.get_loc(column_name)
                except (KeyError, TypeError):
                    continue

                #print column_index, " column_name " , column_name

                for idxn, index_name in enumerate(index_tuples):

                    if idxn % len(constraint_dict) == con:

                        try:
                            if len(index_name) == 1:
                                row_index = data_frame_appoggio_righe.index.get_loc(index_name[0])
                            else:
                                row_index = data_frame_appoggio_righe.index.get_loc(index_name)
                        except (KeyError, TypeError):
                            continue

                        #print row_index, " index_name " , index_name

                        constraint_val = row[new_header.index(alias)]
                        src_row = data[row_index]
                        val = src_row[column_index]

                        if range == True:
                            src_row[column_index] = get_class_range(val)
                        else:
                            src_row[column_index] = ASTERISK

                        if debug:
                            src_row[column_index] += "(%s, " % val
                            src_row[column_index] += "%s" % enum_column
                            src_row[column_index] += "=%s)" % constraint_val

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
        if r > len(data_frame.index) - 1:
            continue
        index = data_frame.index[r]
        for c, o in enumerate(data_frame.columns):
            value = data[r][c]
            ret.set_value(index, data_frame.columns[c], value)
    return ret


def apply_constraint_plain(data,
                           col_dict,
                           constraint_cols,
                           debug,
                           aggregation):
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
                                                   dict(),
                                                   aggregation)

        st = detect_special_columns(query)
        query, h = build_description_query(query, st.cols, [], False, False)

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


def column_secondary_plain_suppression(data, debug):
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
    row_limit = len(data) - 1
    if len(data) <= 3:
        return data, asterisked

    for c, column in enumerate(data[0]):
        asterisk = 0
        if c == len(data[0]):
            break

        for row in data:
            val = str(row[c])
            if val.startswith("*"):
                asterisk += 1
            if asterisk > 1:
                break

        if asterisk == 1:
            min_r = find_row_with_min_value(data,
                                            0,
                                            row_limit,
                                            c)
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
                                                             row_limit,
                                                             c)
                if min_r is not None:
                    value = data[min_r][c]
                    data[min_r][c] = ASTERISK
                    if debug:
                        data[min_r][c] += ASTERISK + 'C(' + str(value) + ")"
                    asterisked += 1

    return data, asterisked


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

    data, asterisked_c = column_secondary_plain_suppression(data, debug)

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
                            debug,
                            aggregation):
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
                                  debug,
                                  aggregation)

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
                                         debug,
                                         obs_vals,
                                         aggregation,
                                         old_cols,
                                         agg_filters,
                                         range):
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
    constraint_values, table, enum_column, query, new_header = build_secondary_query(secondary,
                                                                  col_dict,
                                                                  filters,
                                                                  old_cols,
                                                                  aggregation)

    st = detect_special_columns(query)

    #print query
    #print "new_header " , new_header
    #print "constraint_values " , constraint_values

    if query is None:
        return data, asterisk_global_count

    alias = get_column_description(table, enum_column)

    if len(aggregation) > 0:

        #torno agli elementi originali perche qui le colonne sono gia' raggruppate
        elemento = dict()
        elemento[0] = enum_column

        dest_table_description = get_table_schema(table)

        dest_columns = [field.name for field in dest_table_description]

        cols = dict()

        cols[0] = dict()
        cols[0]['table'] = table
        cols[0]['column'] = enum_column

        indice = 1

        for col in old_cols:

            col_name = old_cols[col]['column']

            if col_name in dest_columns:
                cols[indice] = dict()
                cols[indice]['table'] = table
                cols[indice]['column'] = old_cols[col]['column']
                indice += 1

        query, err = build_aggregation_query(query,
                                             cols,
                                             aggregation,
                                             agg_filters,
                                             elemento,
                                             constraint_values)
        st = detect_special_columns(query)

    query, new_header = build_description_query(query,
                                                st.cols,
                                                pivot_columns,
                                                False,
                                                False)
    query += "\n ORDER BY \"%s\"" % new_header[len(new_header) - 1]

    #print query

    #print "new_header " , new_header

    dest_data = execute_query_on_main_db(query)

    if has_data_frame_multi_level_columns(data_frame):  #riordina le colonne .......
        data_frame_appoggio_colonne = data_frame.sortlevel(axis=1)
    else:
        data_frame_appoggio_colonne = data_frame.sort_index(axis=1)

    if len(obs_vals) == 1:  #se ce' un solo obsvalue finisce in colonna .... forse e' generalizzabile anche per un caso che non sia il turismo
        data_frame_appoggio_colonne.drop((','.join(data_frame_appoggio_colonne.columns.levels[0]), TOTAL), axis=1, inplace=True)  # e poi tolgo la label TOTALE sulle colonne
    else:
        data_frame_appoggio_colonne.drop(TOTAL, axis=1,inplace=True)  # e poi tolgo la label TOTALE sulle colonne

    slice_da_preservare_colonne, col_tuples = estrai_tuple_per_colonne(data_frame_appoggio_colonne,True, obs_vals)  #True perche devo tutelare gli slices di colonna

    if has_data_frame_multi_level_index(data_frame):  #riordina il data_freame per le righe...
        data_frame_appoggio_righe = data_frame.sortlevel(axis=0)
    else:
        data_frame_appoggio_righe = data_frame.sort_index(axis=0)

    data_frame_appoggio_righe.drop(TOTAL, axis=0, inplace=True)  # e poi tolgo la label TOTALE sulle righe

    slice_da_preservare_righe, index_tuples = estrai_tuple_per_righe(data_frame_appoggio_righe,False, obs_vals)  #false percghe mi servono tutte le righe e non mi servono gli slices di riga

    """
    print "data_frame_appoggio_colonne "
    print data_frame_appoggio_colonne

    print slice_da_preservare_colonne
    print "col_tuples " , col_tuples

    print slice_da_preservare_righe
    print "index_tuples " , index_tuples
    """

    for rt, row_tup in enumerate(index_tuples):

        try:
            row_index = data_frame_appoggio_righe.index.get_loc(index_tuples[rt])
        except (KeyError, TypeError):
            continue
        src_row = data[row_index]

        #print row_tup, "row_index " , row_index

        for ct, col_tup in enumerate(col_tuples):

            try:
                column_index = data_frame_appoggio_colonne.columns.get_loc(col_tuples[ct])
            except (KeyError, TypeError):
                continue

            #print col_tup, "column_index " , column_index

            if not isinstance(column_index, slice):
                start_col = 0
                stop_col = len(src_row) - 1
            else:
                start_col = column_index.start
                stop_col = column_index.stop

            sel_col = start_col
            asterisk_count = 0
            while sel_col != stop_col:
                if str(src_row[sel_col]).startswith(ASTERISK):
                    asterisk_count += 1
                sel_col += 1

            sel_col = start_col

            if asterisk_count == 1:

                """
                print row_tup, "row_index " , row_index
                print col_tup, "column_index " , column_index
                print "asterisk_count " , asterisk_count
                """

                # Is to be asterisked an other one.

                #ricavo la lista della tuple da cercare nella query di appoggio con il numero di strutture
                cell_col_list = []
                cell_col_value = []

                #print "col_tup " , col_tup
                #print "row_tup ", row_tup, " ", len(row_tup)
                #print "data_frame.index.names ", data_frame.index.names
                #print "data_frame.columns.names ", data_frame.columns.names
                #print "new_header " , new_header

                for i, index in enumerate(data_frame.index.names):
                    if index in new_header:
                        cell_col_list.append(new_header.index(index))
                        if isinstance(row_tup, tuple):
                            cell_col_value.append(row_tup[i])
                        else:
                            cell_col_value.append(row_tup)

                for i, column in enumerate(data_frame.columns.names):
                    if i < slice_da_preservare_colonne:
                        if column in new_header:
                            cell_col_list.append(new_header.index(column))
                            if isinstance(col_tup, tuple):
                                cell_col_value.append(col_tup[i])
                            else:
                                cell_col_value.append(col_tup)

                #print "slice_da_preservare_colonne ", slice_da_preservare_colonne
                #print "cell_col_list ", cell_col_list
                #print "cell_col_value ", cell_col_value

                minimo = sys.maxint

                for d_r, target_row in enumerate(dest_data):  #scorre la query

                    elementi = 0
                    for i, column in enumerate(cell_col_list):
                        if to_utf8(target_row[column]) == cell_col_value[i]:
                            elementi += 1

                    if elementi == len(cell_col_list):

                        if target_row[new_header.index(alias)] < minimo:
                            minimo = target_row[new_header.index(alias)]

                            #print "minimooooooo " , minimo

                            indice_minimo = []

                            for i, column in enumerate(data_frame.columns.names):
                                if i >= slice_da_preservare_colonne:
                                    if column in new_header:
                                        #print "column ", column
                                        indice_minimo.append(target_row[new_header.index(column)])


                #print "minimo " , minimo

                if minimo == sys.maxint:  #non ho trovato nulla ..... quindi vuol dire che in quello slice non ho altri dati quindi asterisco il primo che trovo

                    while sel_col != stop_col:
                        if not str(src_row[sel_col]).startswith(ASTERISK):
                            cell = str(src_row[sel_col])
                            src_row[sel_col] = ASTERISK

                            if debug:
                                src_row[sel_col] += ASTERISK
                                src_row[sel_col] += 'R(' + cell + ","  + enum_column + " NON PRESENTE)"

                            break

                        sel_col += 1

                else:
                    #print "indice_minimo " , indice_minimo
                    if isinstance(column_index, slice):
                        #print "a"
                        #print "slice_da_preservare_colonne " , slice_da_preservare_colonne

                        appoggio = []

                        for q, qcolumn in enumerate(col_tup):
                            #print q, "qcolumn " , qcolumn

                            if len(obs_vals) == 1:
                                appoggio.append(qcolumn)
                            else:
                                if q < slice_da_preservare_colonne:
                                    appoggio.append(qcolumn)

                        colonna = tuple(appoggio)

                        colonna += tuple(indice_minimo)
                    else:
                        #print "b"
                        if len(indice_minimo) == 1:
                            colonna = indice_minimo[0]
                        else:
                            colonna = tuple(indice_minimo)


                    #print "colonna ", colonna

                    indice_colonna = data_frame_appoggio_colonne.columns.get_loc(colonna)

                    cell = str(src_row[indice_colonna])
                    src_row[indice_colonna] = ASTERISK
                    if debug:
                        src_row[indice_colonna] += ASTERISK
                        src_row[indice_colonna] += 'R(' + cell + ","  + enum_column + "=" +  str(minimo) + ")"


    #print query

    return data, asterisk_global_count

def secondary_col_suppression_constraint(data,
                                         data_frame,
                                         pivot_columns,
                                         rows,
                                         col_dict,
                                         obs_vals,
                                         secondary,
                                         filters,
                                         debug,
                                         aggregation,
                                         old_cols,
                                         agg_filters,
                                         apply_range):
    """
    Performs secondary suppression on columns following the secondary metadata
    rule on table.

    :param data_frame: Data frame.
    :param pivot_columns: Pivot columns.
    :param rows: Rows.
    :param col_dict: Columns dictionary.
    :param obs_vals: Observable values.
    :param secondary: Constraint for secondary suppression.
    :param filters: Filters.
    :param debug: Is to be debugged?
    :return: data, number of asterisk.
    """

    #return data, 0

    asterisk_global_count = 0

    constraint_values, table, enum_column, query, new_header = build_secondary_query(secondary,
                                                                  col_dict,
                                                                  filters,
                                                                  old_cols,
                                                                  aggregation)

    st = detect_special_columns(query)

    #print query
    #print "new_header " , new_header
    #print "constraint_values " , constraint_values

    if query is None:
        return data, asterisk_global_count

    alias = get_column_description(table, enum_column)

    if len(aggregation) > 0:

        #torno agli elementi originali perche qui le colonne sono gia' raggruppate
        elemento = dict()
        elemento[0] = enum_column

        dest_table_description = get_table_schema(table)

        dest_columns = [field.name for field in dest_table_description]

        cols = dict()

        cols[0] = dict()
        cols[0]['table'] = table
        cols[0]['column'] = enum_column

        indice = 1

        for col in old_cols:

            col_name = old_cols[col]['column']

            if col_name in dest_columns:
                cols[indice] = dict()
                cols[indice]['table'] = table
                cols[indice]['column'] = old_cols[col]['column']
                indice += 1

        query, err = build_aggregation_query(query,
                                             cols,
                                             aggregation,
                                             agg_filters,
                                             elemento,
                                             constraint_values)
        st = detect_special_columns(query)

    query, new_header = build_description_query(query,
                                                st.cols,
                                                pivot_columns,
                                                False,
                                                False)
    query += "\n ORDER BY \"%s\"" % new_header[len(new_header) - 1]

    #print query
    #print "new_header " , new_header

    dest_data = execute_query_on_main_db(query)

    data_frame_appoggio = pd.DataFrame(data_frame.values.copy(), data_frame.index.copy(), data_frame.columns.copy())

    data_frame_appoggio = remove_code_from_data_frame(data_frame_appoggio)

    if has_data_frame_multi_level_index(data_frame_appoggio):  #riordina il data_freame per le righe...
        data_frame_appoggio = data_frame_appoggio.sortlevel(axis=0)
    else:
        data_frame_appoggio = data_frame_appoggio.sort_index(axis=0)

    #print data_frame_appoggio.index.values.tolist()

    data_frame_appoggio.drop(TOTAL, axis=0, inplace=True)  # e poi tolgo la label TOTALE sulle righe

    #print data_frame_appoggio.index.values.tolist()

    #return data, 0

    if len(obs_vals) > 1:
        slice_da_preservare = len(data_frame_appoggio.index.levels) - 2  # 2, 1 per l'obs value e uno per l'ultimo slices
    else:
        if has_data_frame_multi_level_index(data_frame_appoggio):
            slice_da_preservare = len(data_frame_appoggio.index.levels) - 1  #uno per l'ultimo slices
        else:
            slice_da_preservare = 0

    #print "slice_da_preservare ", slice_da_preservare

    #index_tuples = tuple(data_frame_appoggio.index.levels[slice_da_preservare-1])
    #print index_tuples

    if slice_da_preservare == 0:

        start_row = 0
        stop_row = len(data) - 1

        for c, col in enumerate(data_frame_appoggio.columns):

            try:
                column_index = data_frame_appoggio.columns.get_loc(col)
            except (KeyError, TypeError):
                continue

            #print "col " , col
            #print "column_index " , column_index

            sel_row = start_row
            asterisk_count = 0

            while sel_row != stop_row:
                if str(data[sel_row][column_index]).startswith(ASTERISK):
                    asterisk_count += 1
                sel_row += 1

            sel_row = start_row

            if asterisk_count == len(obs_vals):

                cell_col_list = []
                cell_col_value = []

                #print "new_header " , new_header

                for i, column in enumerate(data_frame_appoggio.columns.names):
                    #print "column " , column
                    if column in new_header:
                        cell_col_list.append(new_header.index(column))
                        if isinstance(col, tuple):
                            cell_col_value.append(col[i])
                        else:
                            cell_col_value.append(col)

                #print "cell_col_list ", cell_col_list
                #print "cell_col_value ", cell_col_value

                """ in questo caso non serve
                for i, index in enumerate(data_frame.index.names):
                    if i < slice_da_preservare:
                        if index in new_header:
                            cell_col_list.append(new_header.index(index))
                            #if isinstance(row_tup, tuple):
                            #    cell_col_value.append(row_tup[i])
                            #else:
                            #    cell_col_value.append(row_tup)

                print "cell_col_list ", cell_col_list
                print "cell_col_value ", cell_col_value
                """

                minimo = sys.maxint

                for d_r, target_row in enumerate(dest_data):  #scorre la query

                    elementi = 0
                    for i, column in enumerate(cell_col_list):
                        if to_utf8(target_row[column]) == cell_col_value[i]:
                            elementi += 1

                    if elementi == len(cell_col_list):

                        if target_row[new_header.index(alias)] < minimo:
                            minimo = target_row[new_header.index(alias)]

                            #print "minimooooooo " , minimo

                            indice_minimo = []

                            for i, index in enumerate(data_frame_appoggio.index.names):
                                if i >= slice_da_preservare:
                                    if index in new_header:
                                        #print "column ", column
                                        indice_minimo.append(target_row[new_header.index(index)])

                #print "minimo " , minimo

                if minimo == sys.maxint:  #non ho trovato nulla ..... quindi vuol dire che in quello slice non ho altri dati quindi asterisco il primo che trovo

                    while sel_row != stop_row:
                        if not str(data[sel_row][column_index]).startswith(ASTERISK):

                            #print range(1, len(obs_vals) + 1)
                            #print len(obs_vals)

                            for i in range(1, len(obs_vals) + 1):
                                #print "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii", i
                                asterisk_global_count += 1
                                cell = str(data[sel_row][column_index])
                                data[sel_row][column_index] = ASTERISK

                                if debug:
                                    data[sel_row][column_index] += ASTERISK
                                    data[sel_row][column_index] += 'C(' + cell + ","  + enum_column + " NON PRESENTE)"

                                sel_row += 1

                            break

                        sel_row += 1
                else:
                    #print "indice_minimo " , indice_minimo

                    riga = indice_minimo[0]

                    lista = []
                    lista.append([riga])

                    livello = data_frame_appoggio.index.levels[len(data_frame_appoggio.index.levels) - 1]

                    lista.append(livello.tolist())

                    #print bcolors.FAIL

                    #print "lista ", lista

                    lista_appoggio = list(itertools.product(*lista))

                    #print "lista_appoggio " , lista_appoggio

                    for ct2, row_tup2 in enumerate(lista_appoggio):


                        #print "row_tup2 " , row_tup2[0]
                        #print "row_tup2 " , row_tup2[0].encode('ascii','ignore')
                        #print data_frame_appoggio.index

                        try:
                            indice_riga = data_frame_appoggio.index.get_loc(row_tup2)
                        except (KeyError, TypeError):
                            continue

                        #indice_riga = data_frame_appoggio.index.get_loc(tuple(appoggio))
                        asterisk_global_count += 1

                        cell = str(data[indice_riga][column_index])
                        data[indice_riga][column_index] = ASTERISK
                        if debug:
                            data[indice_riga][column_index] += ASTERISK
                            data[indice_riga][column_index] += 'C(' + cell + ","  + enum_column + "=" +  str(minimo) + ")"

    else:  #else del if slice_da_preservare == 0:
        #for c, col in enumerate(data_frame_appoggio.index.levels):
        #    if c < slice_da_preservare:
        #        print c, col

        index_tuples = tuple(data_frame_appoggio.index.levels[slice_da_preservare-1])

        index_tuples = tuple([x for x in index_tuples if x != TOTAL])

        #print "index_tuples " , index_tuples

        #print data_frame.index.get_loc((12,'Altipiani Cimbri                                  '))
        #print data_frame_appoggio.index.get_loc('Altipiani Cimbri                                  ')

        for ct, row_tup in enumerate(index_tuples):

            #print row_tup

            #print data_frame.index.get_loc(('Alta Valsugana e Bersntol                         '))

            try:
                row_index = data_frame_appoggio.index.get_loc(row_tup)  #il data_frame non posso usarlo perche li ho i codici
            except (KeyError, TypeError):
                continue

            #print bcolors.FAIL
            #print "row_tup " , row_tup
            #print "row_index " , row_index

            start_row = row_index.start
            stop_row = row_index.stop

            for c, col_tup in enumerate(data_frame_appoggio.columns):


                try:
                    column_index = data_frame_appoggio.columns.get_loc(col_tup)
                except (KeyError, TypeError):
                    continue

                #print "col_tup " , col_tup
                #print "column_index " , column_index
                #print bcolors.ENDC

                sel_row = start_row
                asterisk_count = 0

                while sel_row != stop_row:
                    if str(data[sel_row][column_index]).startswith(ASTERISK):
                        asterisk_count += 1
                    sel_row += 1

                sel_row = start_row

                if asterisk_count == len(obs_vals):

                    #print "col_tup " , col_tup
                    #print "row_tup " , row_tup

                    cell_col_list = []
                    cell_col_value = []

                    for i, column in enumerate(data_frame_appoggio.columns.names):
                        #print "column " , column
                        if column in new_header:
                            cell_col_list.append(new_header.index(column))
                            if isinstance(col_tup, tuple):
                                cell_col_value.append(col_tup[i])
                            else:
                                cell_col_value.append(col_tup)

                    for i, index in enumerate(data_frame_appoggio.index.names):
                        if i < slice_da_preservare:
                            if index in new_header:
                                cell_col_list.append(new_header.index(index))
                                if isinstance(row_tup, tuple):
                                    cell_col_value.append(row_tup[i])
                                else:
                                    cell_col_value.append(row_tup)

                    #print "slice_da_preservare ", slice_da_preservare

                    #print "cell_col_list ", cell_col_list
                    #print "cell_col_value ", cell_col_value

                    minimo = sys.maxint

                    for d_r, target_row in enumerate(dest_data):  #scorre la query

                        elementi = 0
                        for i, column in enumerate(cell_col_list):
                            if to_utf8(target_row[column]) == cell_col_value[i]:
                                elementi += 1

                        if elementi == len(cell_col_list):

                            if target_row[new_header.index(alias)] < minimo:
                                minimo = target_row[new_header.index(alias)]

                                #print "minimooooooo " , minimo

                                indice_minimo = []

                                for i, index in enumerate(data_frame_appoggio.index.names):
                                    if i >= slice_da_preservare:
                                        if index in new_header:
                                            #print "column ", column
                                            indice_minimo.append(target_row[new_header.index(index)])

                    #print "minimo " , minimo
                    #print "indice_minimo " , indice_minimo

                    if minimo == sys.maxint:  #non ho trovato nulla ..... quindi vuol dire che in quello slice non ho altri dati quindi asterisco il primo che trovo

                        while sel_row != stop_row:
                            if not str(data[sel_row][column_index]).startswith(ASTERISK):

                                #print len(obs_vals)
                                #print range(1, len(obs_vals) + 1)

                                for i in range(1, len(obs_vals) + 1):
                                    #print "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii", i
                                    #print "dddddddddddddddddddddd 1"
                                    asterisk_global_count += 1
                                    cell = str(data[sel_row][column_index])
                                    data[sel_row][column_index] = ASTERISK

                                    if debug:
                                        data[sel_row][column_index] += ASTERISK
                                        data[sel_row][column_index] += 'C(' + cell + ","  + enum_column + " NON PRESENTE)"

                                    sel_row += 1

                                break

                            sel_row += 1

                    else:

                        #print colors[random.randint(0,len(colors)-1)]
                        #print get_color()

                        #print "indice_minimo " , indice_minimo

                        lista = []

                        for i, index in enumerate(data_frame_appoggio.index.names):
                            #print "index " , index
                            if i < slice_da_preservare:
                                if index in new_header:
                                    if isinstance(row_tup, tuple):
                                        lista.append(row_tup[i])
                                    else:
                                        lista.append([row_tup])

                        #print "lista " , lista

                        riga = indice_minimo[0]

                        lista.append([riga])

                        if len(obs_vals) > 1:
                            livello = data_frame_appoggio.index.levels[len(data_frame_appoggio.index.levels) - 1]
                            lista.append(livello.tolist())

                        #print "lista " , lista

                        lista_appoggio = list(itertools.product(*lista))

                        #print "lista_appoggio " , lista_appoggio

                        for ct2, row_tup2 in enumerate(lista_appoggio):

                            #print row_tup2

                            try:
                                indice_riga = data_frame_appoggio.index.get_loc(row_tup2)
                            except (KeyError, TypeError):
                                continue

                            #print row_tup2, indice_riga

                            #print "dddddddddddddddddddddd 2"
                            asterisk_global_count += 1

                            cell = str(data[indice_riga][column_index])
                            data[indice_riga][column_index] = ASTERISK
                            if debug:
                                data[indice_riga][column_index] += ASTERISK
                                data[indice_riga][column_index] += 'C(' + cell + ","  + enum_column + "=" +  str(minimo) + ")"

                        #print "riga ", riga

    #print "ciaoooooooooo ", asterisk_global_count
    #print query

    return data, 0


def apply_stat_secret(headers,
                      data,
                      col_dict,
                      pivot_dict,
                      secret_column_dict,
                      sec_ref,
                      threshold_columns_dict,
                      constraint_cols,
                      filters,
                      aggregation,
                      debug,
                      visible,
                      include_code,
                      old_cols,
                      agg_filters,
                      range):
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
    :param aggregation:
    :param debug: If active show debug info on asterisked cells.
    :param visible: View all data without put asterisks.
                    In this case it apply pivot only if required.
    :return: data, headers, data_frame, warn, err.
    """

    """
    print "col_dict " , col_dict
    print "pivot_dict " , pivot_dict
    print "secret_column_dict " , secret_column_dict
    print "sec_ref " ,sec_ref
    print "threshold_columns_dict " , threshold_columns_dict
    print "constraint_cols " , constraint_cols
    print "filters " , filters
    print "aggregation " , aggregation
    print "include_code " , include_code
    """

    warn = None
    err = None

    #print "pivot_dict ", pivot_dict

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

        #print "headers " , headers
        #print data

        data, data_frame, err = pivot(data,
                                      headers,
                                      pivot_cols,
                                      rows,
                                      pivot_values)

        #print data_frame

        """
        print err
        print data
        print data_frame
        print "columns " , data_frame.columns
        print "index ", data_frame.index
        """

        if err:
            return rows, headers, None, warn, err

        if len(obs_vals) > 1:
            data_frame = data_frame.stack(0)
            data = get_data_from_data_frame(data_frame)

        #visible = vedi tutto, se vedi tutto ritorna il dataset cosi come e' senza asterischi
        #debug = analizza
        if visible and not debug:
            return data, headers, data_frame, warn, err

        data = apply_constraint_pivot(data,  #primaria per tabelle collegate (turismo)
                                      data_frame,
                                      pivot_dict,
                                      rows,
                                      col_dict,
                                      constraint_cols,
                                      filters,
                                      aggregation,
                                      debug,
                                      obs_vals,
                                      include_code,
                                      old_cols,
                                      range)

        sec = get_table_metadata_value(col_dict[0]['table'], 'secondary')

        if not sec is None and len(sec) > 0:
            tot_asterisked = 1
            while tot_asterisked > 0:  #secondaria per tabelle collegate (turismo)


                data_frame = data_frame_from_tuples(data_frame, data)

                data, ast_r = secondary_row_suppression_constraint(data,
                                                                   data_frame,
                                                                   pivot_dict,
                                                                   rows,
                                                                   col_dict,
                                                                   sec[0][0],
                                                                   filters,
                                                                   debug,
                                                                   obs_vals,
                                                                   aggregation,
                                                                   old_cols,
                                                                   agg_filters,
                                                                   range)


                data_frame = data_frame_from_tuples(data_frame, data)

                data, ast_c = secondary_col_suppression_constraint(data,
                                                                   data_frame,
                                                                   pivot_dict,
                                                                   rows,
                                                                   col_dict,
                                                                   obs_vals,
                                                                   sec[0][0],
                                                                   filters,
                                                                   debug,
                                                                   aggregation,
                                                                   old_cols,
                                                                   agg_filters,
                                                                   range)

                tot_asterisked = ast_c + ast_r


        else:
            data = protect_pivoted_table(data,
                                         #qui fa la primaria e la secodaria per labelle "normali"
                                         data_frame,
                                         secret_column_dict,
                                         sec_ref, # ??? detect_special_columns ... ma non capisco a cosa serve
                                         threshold_columns_dict,
                                         constraint_cols, # cosa serve ??? che e' per il turismo ??? PILLO ti ammazzo
                                         obs_vals,
                                         pivot_dict,
                                         col_dict,
                                         rows,
                                         debug)

        #print "aaaaaaaaaaaaaaaaaaaaaaa"
        data_frame = data_frame_from_tuples(data_frame, data)

        #print "bbbbb"

        return data, headers, data_frame, warn, err

    # If plain and secret does not return it.
    if len(secret_column_dict) > 0 or len(constraint_cols) > 0 or len(sec_ref) > 0:
        return None, headers, None, warn, err

    if len(data) > 0:
        data_frame = pd.DataFrame(data, columns=headers)
    else:
        data_frame = pd.DataFrame(columns=headers)

    return data, headers, data_frame, warn, err


def stampa_symtobltabel(st):
    print "aggregation"
    print st.aggregation
    print "cols"
    print st.cols
    print "constraint"
    print st.constraint
    print "decoder"
    print st.decoder
    print "include_descriptions"
    print st.include_descriptions
    print "pivot"
    print st.pivot
    print "secret"
    print st.secret
    print "secret_ref"
    print st.secret_ref
    print "threshold"
    print st.threshold


def headers_and_data(user,
                     query,
                     filters,
                     aggregation,
                     agg_filters,
                     pivot_cols,
                     debug,
                     include_descriptions,
                     include_code,
                     visible,
                     range):
    """
    Execute query, get headers, data, duration, error
    and filter result set to preserve the statistical secret.

    :param user: User that perform the request.
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
    :param visible: View all data without put asterisks.
    """

    if user.is_authenticated():
        id = user.pk
    else:
        id = -1

    log = ExecutedQueryLog.create(query.title, id)
    log.save()

    warn = None
    df = None
    st = detect_special_columns(query.sql)

    #print "query.sql ", query.sql
    #stampa_symtobltabel(st)

    #print query.sql
    #stampa_symtobltabel(st)

    if pivot_cols is None:
        # If it is not specified then use the standard one.
        # come fa' a essere vuoto se nel query editor devo per forza specificarne uno ????
        pivot_cols = st.pivot

    """
    print "1-----"
    print query.sql
    print "1-----"
    """

    #stampa_symtobltabel(st)

    old_cols = st.cols
    #print "st.threshold ", st.threshold

    if len(aggregation) > 0:
        query.sql, err = build_aggregation_query(query.sql,
                                                 st.cols,
                                                 aggregation,
                                                 agg_filters,
                                                 st.threshold,
                                                 [])
        st = detect_special_columns(query.sql)

    #print "agg_filters " , agg_filters


    """
    print bcolors.OKGREEN
    print "2-----"
    print query.sql
    print "2-----"
    """


    if include_descriptions or st.include_descriptions: # ...... sempre vero ......robe da legnare Pillo
        query.sql, h = build_description_query(query.sql,
                                               st.cols,
                                               pivot_cols,
                                               False,
                                               include_code)
        st = detect_special_columns(query.sql)

    """
    print bcolors.WARNING
    print "3-----"
    print query.sql
    print "3-----"
    print bcolors.ENDC
    """

    old_head, data, duration, err = query.headers_and_data()

    if len(data) == 0:
        return df, data, warn, err

    """
    print "-------------------------"
    print err

    print data
    """

    #stampa_symtobltabel(st)

    add_secret_field_not_selected(filters, st.secret, st.cols[0]['table'])

    #stampa_symtobltabel(st)

    if err is None:
        if len(old_head) < 3 and len(st.secret) + len(st.constraint) + len(st.secret_ref) == 1 and len(st.threshold) == 1:
            #print "bla bla bla"
            data, head, df = apply_stat_secret_plain(old_head,
                                                     #questo credo che non vada piu .... e probailmente non serve nemmeno piu
                                                     data,
                                                     st.cols,
                                                     st.threshold,
                                                     st.constraint,
                                                     debug,
                                                     aggregation)
        # Check if I can give the full result set.
        elif (len(st.secret) + len(st.constraint) + len(st.secret_ref) == 0) or (pivot_cols is not None and len(pivot_cols) > 0):
            #print "bla2 bla2 bla2"
            data, old_head, df, warn, err = apply_stat_secret(old_head,
                                                              data,
                                                              st.cols,
                                                              st.pivot,
                                                              st.secret,
                                                              st.secret_ref,
                                                              st.threshold,
                                                              st.constraint,
                                                              filters,
                                                              aggregation, # serve per le tabelle del turismo per costruire la query secondaria
                                                              debug,
                                                              visible,
                                                              include_code,
                                                              old_cols,
                                                              agg_filters, # serve per le tabelle del turismo per costruire la query secondaria
                                                              range)

    if not df is None:
        if not include_code:
            # Fix all columns before drop the codes.
            df = remove_code_from_data_frame(df)

        if contains_ref_period(st.pivot, st.cols, axis=0) or df.shape[1] == 2:
            df = drop_total_column(df)

        index = get_data_frame_first_index(df)
        if contains_ref_period(st.pivot, st.cols, axis=1) or len(index) == 2:
            df = drop_total_row(df)

    #print "cccccccccccccc"

    return df, data, warn, err


def generate_storage_id():
    """
    Get a new storage id.

    :return: Id fo build storage.
    """
    storage_id = "%d" % uuid.uuid4()
    return storage_id


def get_session_filename():
    """
    Get a temporary filename associated to the request.

    :return: Session file name.
    """
    session_id = str(generate_storage_id())
    sys_temp = tempfile.gettempdir()
    store_name = "%s.pkl" % session_id
    store_name = os.path.join(sys_temp, store_name)
    return store_name


def load_data_frame(request):
    """
    Load the data frame from the file associated to the request.

    :param request: Http request.
    :return: Data frame.
    """
    store_name = request.REQUEST.get('store', '')
    if store_name is "":
        query_id = request.REQUEST.get('id')
        query = Query.objects.get(id=query_id)
        if query.open_data:
            st = detect_special_columns(query.sql)
            query.sql, h = build_description_query(query.sql,
                                                   st.cols,
                [],
                                                   False,
                                                   True)
            head, data, duration, err = query.headers_and_data()
            df = pd.DataFrame(data, columns=head)
            return df
        else:
            if not query.is_public:
                return None
            filters = request.REQUEST.get('filters')
            if filters is None and query.query_editor:
                filters = json.loads(query.filters)
            agg_filters = request.REQUEST.get('agg_filters')
            if agg_filters is None and query.query_editor:
                agg_filters = json.loads(query.agg_filters)

            # Preform standard pivot.
            pivot_cols = None
            aggregation = request.REQUEST.get('aggregate', "")
            if query.query_editor and \
                    (aggregation is None or aggregation == ""):
                aggregation = query.aggregations

            aggregation_ids = []
            if aggregation is not None and aggregation != "":
                aggregation_ids = [ast.literal_eval(x) for x in
                                   aggregation.split(',')]

            df, data, warn, err = headers_and_data(request.user,
                                                   query,
                                                   filters,
                                                   aggregation_ids,
                                                   agg_filters,
                                                   pivot_cols,
                                                   False,
                                                   True,
                                                   False,
                                                   False)
            return df
    df = pd.read_pickle(store_name)
    return df


def store_data_frame(df):
    """
    Store in a temporary file associated to the request the Data frame.

    :param df: Data frame.
    """
    store_name = get_session_filename()
    if os.path.exists(store_name):
        os.remove(store_name)
    df.to_pickle(store_name)
    return store_name

