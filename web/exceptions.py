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
Exceptions for l4s project.
"""

from django.utils.translation import ugettext as _


class MissingMetadataException(Exception):
    def __init__(self, key, value, table):
        """
        Override exception init for the custom exception.

        :param key: Missing metadata key
        :param value: Missing key value.
        :param table: Table where is missing the metadata.
        """
        error = unicode(_("Please add the metadata "))
        error += " " + unicode(_("with key")) + " '" + key + "' "
        error += unicode(_("and")) + " "
        error += unicode(_("value")) + " '" + value + "' "
        error += unicode(_("on one of the columns of the table"))
        error += " '" + table + "'"
        self.message = error
