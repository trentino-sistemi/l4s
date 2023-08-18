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
Functions for l4s project to be used with dictionary in Django templates.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

from django import template
register = template.Library()


@register.filter(name='lookup')
def lookup(dictionary, arg):
    """ Lookup on a dictionary.

    :param dictionary: dictionary
    :param arg: key
    :return: value
    """
    return dictionary[arg]


@register.filter(name='lookup_unicode')
def lookup_unicode(dictionary, arg):
    """ Lookup on a dictionary, using as key the arg converted in unicode.

    :param dictionary: dictionary
    :param arg: key
    :return: value
    """
    return dictionary[str(arg)]

register.simple_tag(lookup)
register.simple_tag(lookup_unicode)

