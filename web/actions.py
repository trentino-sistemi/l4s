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
from web.statistical_secret import headers_and_data, get_data_from_data_frame
import StringIO
from rdf import rdf_report
from sdmx import sdmx_report
from tempfile import NamedTemporaryFile
from pandas import ExcelWriter
from web.pyjstat import to_json_stat
from web.utils import unpivot


def generate_report_action_xlsx(aggregation, pivot_cols, debug):
    """
    Generate Excel 2007 file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param pivot_cols: Columns in pivoted table.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with Excel 2007 attachment.
    """
    def generate_report(query):
        """Generate Excel 2007 file from query.

        :param query: Explorer query.
        :return: Response with Excel 2007 attachment.
        """
        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        title = query.title.encode("UTF-8").replace(" ", '_')
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
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        # Add content and return response
        return response

    return generate_report


def generate_report_action_xls(aggregation, pivot_cols, debug):
    """
    Generate Excel 1997 file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param pivot_cols: Columns in pivoted table.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with Excel 1997 attachment.
    """
    def generate_report(query):
        """Generate Excel  1997 file from query.

        :param query: Explorer query.
        :return: Response with Excel 1997 attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        # Limit the columns to the maximum allowed in Excel 97.
        max_length = 255
        index_len = len(df.index.names)

        df = df.drop(
            df.columns[max_length - index_len - 1:len(df.columns) - 1], axis=1)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'xls'
        engine = 'xlwt'
        encoding = 'utf-8'
        content_type = 'application/vnd.ms-excel'
        filename = '%s.%s' % (title, extension)
        # Add content and return response
        f = NamedTemporaryFile(suffix=extension)
        ew = ExcelWriter(f.name, engine=engine, encoding=encoding)
        df.to_excel(ew)
        ew.save()
        # Setup response
        data = f.read()
        response = HttpResponse(data)
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_csv(aggregation,
                               pivot_cols,
                               debug):
    """
    Generate CSV file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param pivot_cols: Columns in pivoted table.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with CSV attachment.
    """
    def generate_report(query):
        """
        Generate Csv file from query.

        :param query: Explorer query.
        :return: Response with CSV attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'csv'
        separator = ';'
        filename = '%s.%s' % (title, extension)
        content_type = 'text/csv'
        out_stream = StringIO.StringIO()
        df.to_csv(out_stream, sep=separator, index=False)
        # Setup response
        response = HttpResponse(out_stream.getvalue())
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_json_stat(aggregation,
                                     threshold,
                                     constr,
                                     pivot_cols,
                                     debug):
    """
    Generate JSON-stat file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param threshold: The column name with threshold this contains the value.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with CSV attachment.
    """
    def generate_report(query):
        """
        Generate JSON-stat file from query.

        :param query: Explorer query.
        :return: Response with JSON-stat attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        if len(pivot_cols) > 0:
            df, data = unpivot(df, query.sql)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'json'
        filename = '%s.%s' % (title, extension)
        content_type = 'application/json'

        value = None

        if len(threshold.keys()) > 0:
            value = df.columns[threshold.keys()[0]]
        elif len(constr) > 0:
            index = constr.keys()[0]
            value = df.columns[index]

        response = HttpResponse()
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)

        if value is None:
            return response

        val = to_json_stat(df, value=value)
        # Setup response
        response.write(val)

        return response

    return generate_report


def generate_report_action_sdmx(aggregation, pivot_cols, debug):
    """
     Generate SDMX file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with SDMX attachment.
    """

    def generate_report(query):
        """
        Generate Sdmx file from query.

        :param query: Explore query.
        :return: Response with Sdmx attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        if len(pivot_cols) > 0:
            df, data = unpivot(df, query.sql)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'sdmx'
        filename = '%s.%s' % (title, extension)
        content_type = 'application/xml'
        res = sdmx_report(query, debug, data=data, data_frame=df)
        # Setup response
        response = HttpResponse(res)
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_rdf(aggregation, pivot_cols, debug):
    """
    Generate RDF file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param pivot_cols: Columns in pivoted table.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with RDF attachment.
    """

    def generate_report(query):
        """
        Generate Rdf file from query.

        :param query: Explorer query.
        :return: Response with RDF attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        if len(pivot_cols) > 0:
            df, data = unpivot(df, query.sql)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'xml'
        filename = '%s.%s' % (title, extension)
        res = rdf_report(query, title, debug, data=data, data_frame=df,
                         rdf_format='xml')
        # Setup response
        content_type = 'application/rdf+xml'
        response = HttpResponse(res)
        response["content_type"] = content_type

        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report


def generate_report_action_turtle(aggregation, pivot_cols, debug):
    """
    Generate TURTLE file from SQL query.

    :param aggregation: Indicate if the query must be aggregate.
    :param pivot_cols: Columns in pivoted table.
    :param debug: Show the clear data and why they are asterisked.
    :return: Response with TURTLE attachment.
    """

    def generate_report(query):
        """
        Generate Turtle file from query.

        :param query: Explorer query.
        :return: Response with Turtle attachment.
        """

        df, data, warn, err = headers_and_data(query,
                                               aggregation,
                                               dict(),
                                               pivot_cols,
                                               debug,
                                               False)

        if len(pivot_cols) > 0:
            df, data = unpivot(df, query.sql)

        title = query.title.encode("UTF-8").replace(" ", '_')
        extension = 'ttl'
        filename = '%s.%s' % (title, extension)
        res = rdf_report(query, title, debug,
                         data=data, data_frame=df, rdf_format='turtle')
        # Setup response
        content_type = 'text/turtle'
        response = HttpResponse(res)
        response["content_type"] = content_type
        response['Content-Disposition'] = \
            'attachment; filename="{0}"'.format(filename)
        return response

    return generate_report
