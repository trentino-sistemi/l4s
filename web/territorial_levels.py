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
Routines for territorial levels..
"""

from web.models import TerritorialLevel


def build_territorial_levels_choice():
    """
    Return a list of tuples with topics.

    :return: List of tuples with topics.
    """

    territorial_levels_list = []
    topic_tuple = ("", "")
    territorial_levels_list.append(topic_tuple)
    objects = TerritorialLevel.objects.all()
    for obj in objects:
        topic_tuple = (obj.name, obj.name)
        territorial_levels_list.append(topic_tuple)

    return territorial_levels_list
