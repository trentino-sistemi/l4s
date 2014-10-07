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
Actions for l4s project.
"""

from django.http import HttpResponse
import StringIO
from rdf import rdf_report
from sdmx import sdmx_report
from tempfile import NamedTemporaryFile
from pandas import ExcelWriter
from web.pyjstat import to_json_stat
from web.utils import unpivot, is_dataframe_multi_index
import pandas as pd


def generate_report_action_csv(df):
    """
    Generate CSV file from SQL query.
    :param df: Pandas data frame
    """
    def generate_report(title):
        """
        Generate Csv file from query.

        :param title: The query title.
        :return: Response with CSV attachment.
        """

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'csv'
        separator = ';'
        filename = '%s.%s' % (title, extension)
        content_type = 'text/csv'
        out_stream = StringIO.StringIO()
        df.to_csv(out_stream, sep=separator, index=False)
        # Setup response
        response = HttpResponse(out_stream.getvalue())
        response["Content-Type"] = content_type
        response[
            'Content-Disposition'] = 'attachment; filename="%s"' % filename
        return response

    return generate_report


def generate_report_action_xls(df):
    """
    Generate Excel 1997 file from SQL query.

    :param df: Pandas data frame.
    :return: Response with Excel 1997 attachment.
    """
    def generate_report(title):
        """Generate Excel  1997 file from query.

        :return: Response with Excel 1997 attachment.
        """

        # Limit the columns to the maximum allowed in Excel 97.
        max_length = 255
        index_len = len(df.index.names)

        lim_df = df.drop(
            df.columns[max_length - index_len - 1:len(df.columns) - 1], axis=1)

        extension = 'xls'
        engine = 'xlwt'
        encoding = 'utf-8'
        content_type = 'application/vnd.ms-excel'
        title = title.strip().encode("UTF-8").replace(" ", '_')
        filename = '%s.%s' % (title, extension)
        # Add content and return response
        f = NamedTemporaryFile(suffix=extension)
        ew = ExcelWriter(f.name, engine=engine, encoding=encoding)
        lim_df.to_excel(ew)
        ew.save()
        # Setup response
        data = f.read()
        response = HttpResponse(data)
        response["Content-Type"] = content_type
        response['Content-Transfer-Encoding'] = 'binary'
        response[
            'Content-Disposition'] = 'attachment; filename="%s"' % filename
        return response

    return generate_report


def generate_report_action_xlsx(df):
    """
    Generate Excel 2007 file from SQL query.

    :param df: Pandas data frame.
    :return: Response with Excel 2007 attachment.
    """
    def generate_report(title):
        """Generate Excel 2007 file from query.

        :return: Response with Excel 2007 attachment.
        """

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'xlsx'
        engine = "openpyxl"
        encoding = 'utf-8'
        filename = '%s.%s' % (title, extension)
        # Add content and return response
        f = NamedTemporaryFile(suffix=extension)
        ew = ExcelWriter(f.name, engine=engine, options={'encoding': encoding})
        df.to_excel(ew)
        ew.save()
        # Setup response
        data = f.read()

        # Setup response
        content_type = \
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        response = HttpResponse(data, content_type=content_type)
        response[
            'Content-Disposition'] = 'attachment; filename="%s"' % filename
        # Add content and return response
        return response

    return generate_report


def generate_report_action_sdmx(df):
    """
     Generate SDMX file from SQL query.

    :param df: Pandas data frame.
    :return: Response with SDMX attachment.
    """

    def generate_report(title, sql):
        """
        Generate Sdmx file from query.

        :param title: The query title.
        :param sql: The query sql.
        :return: Response with Sdmx attachment.
        """

        multi_index = is_dataframe_multi_index(df)
        if multi_index:
            int_df = unpivot(df)
        else:
            int_df = df

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'sdmx'
        filename = '%s.%s' % (title, extension)
        content_type = 'application/xml'
        res = sdmx_report(sql, int_df)
        # Setup response
        response = HttpResponse(res)
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_json_stat(df):
    """
    Generate JSON-stat file from SQL query.

    :param df: Pandas data frame.
    :return: Response with CSV attachment.
    """
    def generate_report(title):
        """
        Generate JSON-stat file from query.

        :return: Response with JSON-stat attachment.
        """
        multi_index = is_dataframe_multi_index(df)
        if multi_index:
            int_df = unpivot(df)
            value = df.columns.levels[0][0]
        else:
            int_df = df
            value = df.columns[len(df.columns)-1]

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'json'
        filename = '%s.%s' % (title, extension)
        content_type = 'application/json'

        response = HttpResponse()
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)

        if value is None:
            return response

        val = to_json_stat(int_df, value=value)
        # Setup response
        response.write(val)

        return response

    return generate_report


def generate_report_action_rdf(df):
    """
    Generate RDF file from SQL query.

    :return: Response with RDF attachment.
    """

    def generate_report(title, description, sql):
        """
        Generate Rdf file from query.

        :param title:  Query title.
        :param description: query description.
        :param sql: Query sql.
        :return: Response with RDF attachment.
        """
        multi_index = is_dataframe_multi_index(df)
        if multi_index:
            int_df = unpivot(df)
        else:
            int_df = df

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'xml'
        filename = '%s.%s' % (title, extension)
        res = rdf_report(sql, title, description,
                         data_frame=int_df,
                         rdf_format='xml')
        # Setup response
        content_type = 'application/rdf+xml'
        response = HttpResponse(res)
        response["content_type"] = content_type

        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_turtle(df):
    """
    Generate TURTLE file from SQL query.

    :param df: Pandas data frame.
    :return: Response with TURTLE attachment.
    """

    def generate_report(title, description, sql):
        """
        Generate Turtle file from query.

        :param title:  Query title.
        :param description: query description.
        :param sql: Query sql.
        :return: Response with Turtle attachment.
        """

        multi_index = is_dataframe_multi_index(df)
        if multi_index:
            int_df = unpivot(df)
        else:
            int_df = df

        title = title.strip().encode("UTF-8").replace(" ", '_')
        extension = 'ttl'
        filename = '%s.%s' % (title, extension)
        res = rdf_report(sql, title, description,
                         data_frame=int_df,
                         rdf_format='turtle')
        # Setup response
        content_type = 'text/turtle'
        response = HttpResponse(res)
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report
