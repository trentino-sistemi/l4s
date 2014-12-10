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
Routines for topics..
"""
from web.utils import execute_query_on_main_db,\
    build_queries_to_tables_mapping


def filter_tables_by_topic(topic_id, tables, order):
    """
    Filter tables on topic.

    :param topic_id: Topic id.
    :param tables: List of tables.
    :return: Tables that belong to topic.
    """
    if tables is None or len(tables) == 0:
        return tables

    tables_str = "'" + "','".join(tables) + "'"
    query = "SELECT a.nome from tabelle a join argomenti_tabelle b \n"
    query += "on (b.id = a.id) "
    query += "WHERE b.argomento=%d " % topic_id
    query += "and a.nome IN (%s) \n" % tables_str
    if not order is None:
        query += "ORDER BY %s" % order

    new_tables = []
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            table_name = row[0]
            new_tables.append(table_name)

    return new_tables


def get_topic_id(table):
    """
    Get the topic number id of the selected table.

    :param table: Table name.
    :return: The topic id.
    """
    query = "SELECT b.argomento from tabelle a join argomenti_tabelle b "
    query += "on (b.id = a.id) WHERE a.nome='%s'" % table
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            return row[0]
    return 999


def get_topic_description(table):
    """
    Get the topic description of the selected table.

    :param table: Table name.
    :return: The topic description.
    """
    query = "SELECT c.descrizione "
    query += "FROM tabelle a, argomenti_tabelle b, argomenti c "
    query += "WHERE b.id = a.id and a.nome='%s' " \
             "and c.argomento=b.argomento" % table

    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            return "%s" % row[0]
    return ""


def has_topic(table, topic):
    """
    Is the topic of the table the topic passed as argument?

    :param table: Table name.
    :param topic: Topic id.
    :return: If the table talk about the topic referenced by id.
    """
    return get_topic_id(table) == topic


def execute_topics_query():
    """
    Execute a query that returns the topics.

    :return: The rows result set.
    """
    query = "SELECT * FROM argomenti"
    rows = execute_query_on_main_db(query)
    return rows


def build_topics_counter_dict(tables):
    """
    Build a dictionary with key topic_id and value the number of table
    belong to the topic.

    :return: Dictionary with key the topic id
             and as value the number of items in topic.
    """
    table_names = "'" + "','".join(tables) + "'"
    topics_counter_dict = dict()
    query = "SELECT c.argomento, COUNT(*)\n"
    query += "FROM tabelle a, argomenti_tabelle b, argomenti c\n"
    query += "WHERE b.id = a.id and c.argomento=b.argomento\n"
    query += "and a.nome IN(%s)" % table_names
    query += "GROUP BY c.argomento"

    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            topics_counter_dict[row[0]] = row[1]
    return topics_counter_dict


def build_topics_decoder_dict():
    """
    Build the topics dict <numeric_value, natural_language_description>.

    :return: Topics dictionary.
    """
    topics_dict = dict()
    rows = execute_topics_query()
    if rows is not None:
        for row in rows:
            key = row[1]
            value = row[2].strip()
            topics_dict[key] = value
    return topics_dict


def build_queries_to_topics_mapping(queries, desired_topic):
    """
    Take in input a list of Query model and return a dictionary
    with key the query id and as value the topics.
    If desired_topic is zero then build the mapping for all topics
    else build the mapping for the desired topic.

    :param queries:
    :return: Query to topic mapping dictionary.
    """

    query_to_tables_mapping = build_queries_to_tables_mapping(queries)
    queries_to_topics = dict()
    for pk in query_to_tables_mapping:
        table_name = query_to_tables_mapping[pk]
        topic = get_topic_id(table_name)
        if topic != 999 and (desired_topic == 0 or topic == desired_topic):
            queries_to_topics[pk] = topic
    return queries_to_topics


def build_topics_dict(tables):
    """
    Build a dictionary with key table name and as value topic.

    :param tables:
    :return:
    """
    ret = dict()
    tables_s = "'" + "','".join(tables) + "'"
    query = "SELECT a.nome, b.argomento from tabelle \n"
    query += "a join argomenti_tabelle b \n"
    query += "on (b.id = a.id) WHERE a.nome IN (%s)" % tables_s
    rows = execute_query_on_main_db(query)
    if not rows is None:
        for row in rows:
            table_name = row[0]
            topic = row[1]
            ret[table_name] = topic
    return ret


def build_topics_choice():
    """
    Return a list of tuples with topics.

    :return: List of tuples with topics.
    """
    topics_list = []
    rows = execute_topics_query()
    if rows is not None:
        for row in rows:
            key = row[1]
            value = row[2]
            topic_tuple = (key, value)
            topics_list.append(topic_tuple)
    return topics_list
