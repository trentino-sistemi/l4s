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
Django admin for l4s project.
"""
from django.db import models
from django.contrib.auth.admin import UserAdmin as _UserAdmin
from web.forms import UserChangeForm, \
    UserCreationForm, \
    UserTypeForm
from web.models import User, Test3, Test4, Test5
from django.contrib import admin


class UserAdmin(_UserAdmin):
    """
    The forms to add and change user instances
    The fields to be used in displaying the User model.
    These override the definitions on the base UserAdmin
    that reference the removed 'username' field
    """
    # The forms to add and change user instances
    form = UserChangeForm
    add_form = UserCreationForm

    # The fields to be used in displaying the User model.
    # These override the definitions on the base UserAdmin
    # that reference specific fields on auth.User.
    list_display = ('email',
                    'first_name',
                    'last_name',
                    'phone_number',
                    'user_type',
                    'is_staff',
                    'is_manual_request_dispatcher')

    list_filter = ('is_staff', 'is_manual_request_dispatcher')
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        ('Personal info', {'fields': ('first_name',
                                      'last_name',
                                      'phone_number',
                                      'user_type')}),
        ('Permissions',
         {'fields': ('is_staff', 'is_manual_request_dispatcher')}),
    )

    # add_fieldsets is not a standard ModelAdmin attribute. UserAdmin
    # overrides get_fieldsets to use this attribute when creating a user.
    add_fieldsets = (
        (None, {'classes': ('wide',),
                'fields': ('email',
                           'first_name',
                           'last_name',
                           'phone_number',
                           'user_type',
                           'password1',
                           'password2')}),
    )
    search_fields = ('email',)
    ordering = ('email',)
    filter_horizontal = ()


class UserType(models.Model):
    """
    Include a custom user type.
    """
    type_form = UserTypeForm


class Test3Admin(admin.ModelAdmin):
    list_display = ('id1', 'id2', 'numerosity')


class Test4Admin(admin.ModelAdmin):
    list_display = ('id1', 'id2', 'id3', 'numerosity')


class Test5Admin(admin.ModelAdmin):
    list_display = ('id1', 'id2', 'id3', 'id4', 'numerosity')


admin.site.register(User, UserAdmin)
admin.site.register(UserType)
admin.site.register(Test3, Test3Admin)
admin.site.register(Test4, Test4Admin)
admin.site.register(Test5, Test5Admin)
