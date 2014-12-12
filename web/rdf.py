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

from rdflib import Graph, URIRef, Literal, BNode, Namespace, term
from rdflib.namespace import RDF, FOAF
from utils import get_metadata_on_column, \
    get_subject_table,\
    get_data_from_data_frame,\
    to_utf8,\
    get_column_description,\
    column_position_in_dataframe,\
    add_xml_header,\
    add_sh_like_header


my_ns = Namespace("http://ontology.trentinosistemi.com/ns/")
cube_ns = Namespace("http://purl.org/linked-data/cube/")
dc_terms = Namespace("http://purl.org/dc/terms/")
sdmx_ns = Namespace("http://purl.org/linked-data/sdmx/2009/")
slice_ref = term.URIRef(cube_ns['slice'])
dataStructureDefinition_ref = term.URIRef(cube_ns['DataStructureDefinition'])
observation_ref = term.URIRef(cube_ns['observation'])
component_ref = term.URIRef(cube_ns['component'])
data_set_ref = term.URIRef(cube_ns['DataSet'])
title_ref = term.URIRef(dc_terms['title'])
description_ref = term.URIRef(dc_terms['description'])
publisher_ref = term.URIRef(dc_terms['publisher'])
subject_ref = term.URIRef(sdmx_ns['subject'])
lang = "it"


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

    :param data_frame: The query header.
    :param sql: The sql  query.
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

    for c in column_dict:
        table_name = column_dict[c]['table']
        column_name = column_dict[c]['column']

        rows = get_metadata_on_column(table_name, column_name)
        for row in rows:
            t_predicate = "%s" % row[3]
            t_object = "%s" % row[4]
            if '#' in t_predicate:
                item = Item(t_predicate.strip(), t_object.strip())
                if c in col_dict:
                    col_dict[c].append(item)
                else:
                    elements = [item]
                    col_dict[c] = elements

    return col_dict


def get_topics(source_tables):
    """
    Get the list of the source tables (the table involved in query) and get
    the list of the involved topics.
    The assumption is that the result set has the union of the topics of the
    source tables.

    :param source_tables: A list of tables.
    :return: The list of topics involved in the tables.
    """
    topics = []
    for table in source_tables:
        rows = get_subject_table(table)
        if rows is not None:
            for row in rows:
                topics.append(row[0])
    return topics


def add_table_triples(description,
                      g,
                      source_tables,
                      table_name,
                      subject_t_ref):
    """
    Add to the graph the list of triples representing the table.

    :param description: Query description.
    :param g: Rdflib Graph.
    :param source_tables: Source tables.
    :param table_name: The table name.
    :param subject_t_ref: The subject reference.
    :return: The Rdflib Graph enriched with table triples.
    """
    title_value = table_name
    publisher_value_ref = term.URIRef(my_ns['sspat'])
    table_description = description

    g.add((subject_t_ref, RDF.type, data_set_ref))
    g.add((subject_t_ref, title_ref, Literal(title_value)))
    g.add((
        subject_t_ref, description_ref, Literal(table_description, lang=lang)))
    g.add((subject_t_ref, publisher_ref, publisher_value_ref))

    topics = get_topics(source_tables)
    for topic in topics:
        topic_ref = URIRef(topic)
        g.add((subject_t_ref, subject_ref, topic_ref))

    g.add((subject_t_ref, RDF.type, publisher_value_ref))
    return g


def add_slice_triples(g, subject_t_ref, data_frame, col_dict):
    """
    Add to the graph the list of triples representing the slice.

    :param g: Rdflib Graph.
    :param subject_t_ref: The subject reference.
    :param data_frame: Pandas data frame.
    :param col_dict: The column dictionary.
    :return:The Rdflib Graph enriched with slices triples.
    """
    for c, column in enumerate(data_frame.columns):
        column = to_utf8(column)
        g.add((subject_t_ref, component_ref, Literal(column.strip())))
        if c not in col_dict:
            continue
        anode = BNode(Literal(column.strip()))
        items = col_dict[c]
        for item in items:
            source = "%s" % item.get_t_predicate()
            dest = "%s" % item.get_t_object()

            if dest.startswith('http://'):
                g.add((anode, URIRef(source), URIRef(dest)))
            else:
                g.add((anode, URIRef(source), Literal(dest, lang=lang)))
    return g


def add_observations_triples(g, data, table_name, slice_t_ref,
                             col_dict):
    """
    Add to the graph the list of triples representing the observations.

    :param g: Rdflib graph.
    :param data: The data set.
    :param table_name: The table name.
    :param slice_t_ref: The slice reference.
    :param col_dict: The column dictionary.
    :return: The Rdflib Graph enriched with the observations triples.
    """
    desc = "http://purl.org/dc/terms/description"
    for r, row in enumerate(data):
        oss_ref = term.URIRef(my_ns["dataset-%s#o%s" % (table_name, r)])
        g.add((slice_t_ref, observation_ref, oss_ref))
        for c in col_dict:
            items = col_dict[c]
            for item in items:
                if item.get_t_predicate().strip() != desc:
                    dest = item.get_t_object()
                    if '#' in dest:
                        val = row[c]
                        value = "%s" % val
                        value = value.strip()
                        if not value.startswith('*'):
                            g.add((oss_ref, URIRef(dest), Literal(value)))
    return g


def get_source_tables(sql):
    """
    Get the source tables.
    Source tables are the tables involved in the query.

    :param sql: The sql query.
    :return: the list of name of the source tables.
    """
    table_list = []
    for line in sql.splitlines():
        left_stripped_line = line.lstrip(' ')
        words = left_stripped_line.split(' ')
        declare = words[0]
        declare_token = '--JOIN'
        if declare == declare_token:
            table_and_column = words[1]
            words = table_and_column.split('.')
            table_name = words[0]
            if table_name not in table_list:
                table_list.append(table_name)
        else:
            continue
    return table_list


def rdf_report(sql,
               title,
               description,
               data_frame=None,
               rdf_format=None):
    """
    Get the rdf report.

    :param sql: The query.
    :param title: The query title.
    :param description: Query description.
    :param data_frame: The Pandas data frame.
    :param rdf_format: The desired rdf format.
    :return: The serialized Rdf.
    """
    g = Graph()
    data = get_data_from_data_frame(data_frame)
    title = title.decode('utf-8')
    dataset = "dataset-%s" % title
    subject_t_ref = term.URIRef(my_ns[dataset])
    source_tables = get_source_tables(sql)

    g = add_table_triples(description, g, source_tables, title, subject_t_ref)
    col_dict = build_column_dict(data_frame, sql)
    slice_t_ref = term.URIRef(my_ns['slice'])

    g = add_slice_triples(g, subject_t_ref,
                          data_frame, col_dict)
    g = add_observations_triples(g,
                                 data,
                                 title,
                                 slice_t_ref,
                                 col_dict)
    ser = g.serialize(format=rdf_format)

    if rdf_format == "xml":
        ser = add_xml_header(ser)
    elif rdf_format == "turtle":
        ser = add_sh_like_header(ser)
    return ser
