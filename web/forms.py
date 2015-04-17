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
Django forms for l4s project.
"""

from django import forms
from django.forms import ModelForm, Field, ValidationError
from django.core.validators import MaxLengthValidator
from django.core.validators import RegexValidator
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm as _UserCreationForm
from django.utils.translation import ugettext_lazy as _
from explorer.models import Query
from l4s.settings import CONTENT_TYPES
from web.models import UserType, Metadata, ManualRequest
from web.topics import build_topics_choice
from web.territorial_levels import build_territorial_levels_choice


class CreateQueryEditorForm(ModelForm):

    class Meta:
        model = Query
        fields = ['title', 'sql', 'description', 'created_by', 'is_public',
                  'query_editor', 'table', 'columns', 'rows', 'obs_values',
                  'aggregations', 'filters', 'agg_filters', 'include_code',
                  'range', 'not_sel_aggregations', 'not_agg_selection_value']

        widgets = {
            'title': forms.Textarea(attrs={'style': 'width: 100%', 'rows': 2}),
            'sql': forms.HiddenInput(),
            'description': forms.Textarea(
                attrs={'style': 'width: 100%', 'rows': 7}),
            'created_by': forms.HiddenInput(),
            'is_public': forms.HiddenInput(),
            'query_editor': forms.HiddenInput(),
            'table': forms.HiddenInput(),
            'columns': forms.HiddenInput(),
            'rows': forms.HiddenInput(),
            'obs_values': forms.HiddenInput(),
            'aggregations': forms.HiddenInput(),
            'filters': forms.HiddenInput(),
            'agg_filters': forms.HiddenInput(),
            'include_code': forms.HiddenInput(),
            'range': forms.HiddenInput(),
            'not_sel_aggregations': forms.HiddenInput(),
            'not_agg_selection_value': forms.HiddenInput()
        }


class CreateQueryForm(ModelForm):

    sql = Field()

    class Meta:
        model = Query
        fields = ['title', 'sql', 'description', 'created_by', 'is_public',
                  'open_data']


class QueryForm(CreateQueryForm):
    """
    This Form extends the sql query form
    in order to do a type checking validation.
    """
    def __init__(self, *args, **kwargs):
        self.variable_dictionary = kwargs.pop('variable_dictionary')
        self.error_msg = kwargs.pop('error_msg')
        super(CreateQueryForm, self).__init__(*args, **kwargs)

    def clean_sql(self):
        """
        Return the cleaned sql.

        :return: valid clean Sql.
        :raise ValidationError:
        """
        parametric_query = self.instance
        cleaned_data = super(QueryForm, self).clean()
        sql = cleaned_data.get('sql')
        if self.error_msg is not None:
            raise ValidationError(
                self.error_msg,
                params={'value': sql},
                code="InvalidSql"
            )

        variable_dictionary = self.variable_dictionary
        available_params = parametric_query.available_params()

        if available_params is not None:

            p = available_params.items()

            for var_name, variable_value in p:
                if variable_dictionary is not None:
                    var_dictionary_item = variable_dictionary.get(var_name)
                    if var_dictionary_item is not None:
                        if isinstance(variable_value[0], list):
                            return sql
                        variable_type = var_dictionary_item.get_type()
                        is_int = variable_type[0] == "integer"
                        if is_int and not variable_value[0].isdigit():
                            msg = _('The variable ')
                            msg += _("%s MUST BE AN INTEGER") % var_name
                            raise ValidationError(
                                msg,
                                params={'value': sql},
                                code="InvalidSql"
                            )

        return sql


class ManualRequestForm(forms.ModelForm):
    """
    Form for Manual Request.
    With this form the user can send manual requests.
    """

    class Meta:

        model = ManualRequest
        widgets = {
            'inquirer': forms.HiddenInput(),
            'dispatcher': forms.HiddenInput(),
            'request_date': forms.HiddenInput(),
            'dispatch_date': forms.HiddenInput(),
            'dispatch_note': forms.HiddenInput(),
            'subject': forms.TextInput(attrs={'style': 'width: 100%'}),
            'goal': forms.TextInput(attrs={'style': 'width: 100%'}),
            'topic': forms.Select(choices=build_topics_choice()),
            'requested_data': forms.Textarea(
                attrs={'style': 'width: 100%', 'rows': 5}),
            'references_years': forms.TextInput(
                attrs={'style': 'width: 100%'}),
            'territorial_level': forms.Select(
                attrs={"onChange": 'javascript:updateTerritorialLevel(this)'},
                choices=build_territorial_levels_choice()),
            'other_territorial_level': forms.TextInput(
                attrs={'disabled': True, 'style': 'width: 100%'}),
            'specific_territorial_level': forms.SelectMultiple(),
            'url': forms.HiddenInput(),
            'dispatched': forms.HiddenInput(),
        }

    def clean_specific_territorial_level(self):
        """
        Clean territorial level

        :return: Comma separated territorial level string.
        """
        import re

        specific_territorial_level = self.cleaned_data.get(
            'specific_territorial_level')
        res = re.findall(r"'([^']*)'", specific_territorial_level)
        res = ','.join(" " + k.rstrip() for k in res)
        return res.lstrip()


class ManualRequestDispatchForm(forms.Form):
    """
    Form to dispatch the manual request.
    """
    widget = forms.Textarea(attrs={'cols': 120, 'rows': 5})
    dispatch_note = forms.CharField(label=_('Dispatch note'), widget=widget)
    dispatcher = forms.CharField(widget=forms.HiddenInput())
    id = forms.CharField(widget=forms.HiddenInput())


class TestForm(forms.Form):
    """
    Form to generate random testy tables.
    """
    TEST_TABLES = (
        ('web_test3', 'test3'), ('web_test4', 'test4'), ('web_test5', 'test5'))

    table_name = forms.ChoiceField(label=_('Table'), widget=forms.Select(
        attrs={"onChange": 'javascript:updateTable(this)'},
    ), choices=TEST_TABLES)
    rows = forms.IntegerField(label=_('Number of rows'))
    min_value = forms.IntegerField(label=_('Minimum value'))
    max_value = forms.IntegerField(label=_('Maximum value'))


class OntologyFileForm(forms.Form):
    """
    Form to upload file.
    """

    upload = forms.FileField()

    def clean_upload(self):
        """
        Clean upload.

        :return: The clean upload.
        :raise forms.ValidationError:
        """
        upload = self.cleaned_data['upload']
        content_type = upload.content_type
        if upload is None or not content_type in CONTENT_TYPES:
            raise forms.ValidationError(_('File type is not supported'))

        return upload


class MetadataForm(forms.ModelForm):
    """
    Form for Metadata.
    You can attach metadata to columns and tables of the main database.
    """
    class Meta:
        model = Metadata
        widgets = {
            'table_name': forms.HiddenInput(),
            'column_name': forms.HiddenInput(),
            'key': forms.TextInput(attrs={'style': 'width: 100%'}),
            'value': forms.TextInput(attrs={'style': 'width: 100%'}),
        }


class MetadataChangeForm(forms.ModelForm):
    """
    Form to change Metadata.
    You can attach metadata to columns and tables of the main database.
    """
    class Meta:
        model = Metadata
        widgets = {
            'table_name': forms.HiddenInput(),
            'column_name': forms.HiddenInput(),
            'key': forms.TextInput(attrs={'style': 'width: 100%'}),
            'value': forms.TextInput(attrs={'style': 'width: 100%'}),
        }


class UserTypeForm(forms.Form):
    """
    A form containing the user type.
    This is used in order to extend the admin site.
    """
    class Meta:
        model = UserType


class UserCreationForm(_UserCreationForm):
    """
    A form to create a new user.
    This is used in order to extend the admin site.
    """
    def __init__(self, *args, **kargs):
        """Constructor of user creation form.

        :param args:
        :param kargs:
        """
        super(UserCreationForm, self).__init__(*args, **kargs)
        del self.fields['username']

    class Meta:
        model = get_user_model()


class UserChangeForm(forms.ModelForm):
    """
    A form to change the user properties.
    This includes only the fields editable by users.
    It is used to change the user profile.
    """
    class Meta:
        model = get_user_model()
        # In this way I select in a simple way the field
        # that will appear in the form.
        fields = ('email',
                  'first_name',
                  'last_name',
                  'phone_number',
                  'user_type')
        widgets = {
            'email': forms.TextInput(attrs={'style': 'width: 100%'}),
            'first_name': forms.TextInput(attrs={'style': 'width: 100%'}),
            'last_name': forms.TextInput(attrs={'style': 'width: 100%'}),
            'phone_number': forms.TextInput(attrs={'style': 'width: 100%'}),
            'user_type': forms.Select(attrs={'style': 'width: 100%'}),
        }


class SignupForm(forms.Form):
    """
    A form for signup new users.
    """
    num = unicode(_('Only numbers are allowed.'))
    eg = unicode(_('e.g.'))
    phone_number = '0461213111'
    pn = "%s %s %s" % (num, phone_number, eg)

    queryset = UserType.objects.order_by('position')

    user_type = forms.ModelChoiceField(queryset,
                                       label='',
                                       required=True,
                                       empty_label=_('User type'))

    first_name = forms.CharField(
        widget=forms.TextInput(
            attrs={
                'placeholder': _('Name')
            }
        ),
        validators=[MaxLengthValidator(30)]
    )

    last_name = forms.CharField(
        widget=forms.TextInput(
            attrs={
                'placeholder': _('Surname')
            }
        ),
        validators=[MaxLengthValidator(32)]
    )

    phone_number = forms.CharField(
        widget=forms.TextInput(
            attrs={
                'placeholder': _('Phone Number'),
            }
        ),
        validators=[
            RegexValidator(
                r'^[0-9]*$',
                pn,
                _('Invalid Phone Number.'),
            ), MaxLengthValidator(16)],
        required=False
    )

    class Meta:
        model = get_user_model()

    def signup(self, request, user):
        """
        Signup request performed by the user.

        :param request: Django request.
        :param user: Django user.
        """
        user.first_name = self.cleaned_data['first_name']
        user.last_name = self.cleaned_data['last_name']
        user.phone_number = self.cleaned_data['phone_number']
        user.user_type = self.cleaned_data['user_type']
        user.save()
