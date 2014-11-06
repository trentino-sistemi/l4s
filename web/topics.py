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
from web.utils import execute_query_on_main_db


def filter_tables_by_topic(topic_id, tables):
    # Filter tables on topic.
    """

    :param topic_id: Topic id.
    :param tables: List of tables.
    :return: Tables that belong to topic.
    """
    new_tables = []
    for table_name in tables:
        if has_topic(table_name, topic_id):
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
            return int(row[0])
    return "999"


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


def build_topics_counter_dict():
    """
    Build a dictionary with key topic_id and value the number of table belong
    to the topic.

    :return: Dictionary with key the topic id
             and as value the number of items in topic.
    """
    topics_counter_dict = dict()
    query = "SELECT c.argomento, COUNT(*)\n"
    query += "FROM tabelle a, argomenti_tabelle b, argomenti c\n"
    query += "WHERE b.id = a.id and c.argomento=b.argomento\n"
    query += "GROUP BY c.argomento"
    rows = execute_query_on_main_db(query)
    if rows is not None:
        for row in rows:
            topics_counter_dict[row[0]] = row[1]
    return topics_counter_dict


def build_topics_dict():
    """
    Build the topics dict <numeric_value, natural_language_description>.

    :return: Topics dictionary.
    """
    topics_dict = dict()
    rows = execute_topics_query()
    if rows is not None:
        for row in rows:
            key = row[1]
            value = row[2]
            topics_dict[key] = value
    return topics_dict


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
