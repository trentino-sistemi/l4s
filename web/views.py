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
Django views for l4s project.
"""

from django.core.mail import send_mail
from copy import deepcopy
from django.db import connections
from django.http import HttpResponseRedirect, HttpResponse
from django.template import RequestContext, Context
from django.utils.decorators import method_decorator
from django.utils.translation import ugettext_lazy as _
from django.views.generic.base import View
from django.views.generic.edit import CreateView
from django.core.exceptions import ObjectDoesNotExist
from django.utils import translation
from django.shortcuts import render_to_response, \
    get_object_or_404, redirect
from django.contrib.auth.decorators import login_required, \
    user_passes_test
from l4s.settings import EXPLORER_RECENT_QUERY_COUNT, \
    EXPLORER_CONNECTION_NAME, \
    LEGEND, DL_ART, \
    DEFAULT_FROM_EMAIL, \
    ADMINISTRATOR_EMAIL, \
    ALLOWED_HOSTS
from explorer.models import Query
from explorer.utils import url_get_rows
from explorer.views import ExplorerContextMixin, \
    view_permission
from web.exceptions import MissingMetadataException
from web.models import Metadata, ManualRequest, OntologyFileModel
from web.forms import QueryForm, \
    UserChangeForm, \
    MetadataForm, \
    MetadataChangeForm, \
    ManualRequestForm, \
    OntologyFileForm, \
    ManualRequestDispatchForm,\
    TestForm, \
    CreateQueryForm, \
    CreateQueryEditorForm
from web.utils import get_variable_dictionary, \
    get_params_dictionary, \
    get_types_dictionary, \
    url_get_parameters, \
    get_widgets_dictionary, \
    build_description_table_dict, \
    build_description_column_dict, \
    list_districts, \
    list_health_districts, \
    list_tourism_sectors, \
    list_valley_communities, \
    email_manual_request_notification, \
    email_new_manual_request, \
    insert_random_data, \
    delete_all, \
    get_table_schema,\
    build_aggregation_query, \
    get_aggregations, \
    get_table_by_name_or_desc, \
    data_frame_to_html, \
    choose_default_axis, \
    build_query, \
    all_obs_value_column, \
    change_query, \
    get_all_aggregations, \
    build_description_query, \
    get_all_field_values, \
    build_query_title, \
    build_query_summary, \
    build_query_desc, \
    list_ref_period, \
    all_hidden_fields, \
    order_tables_by_descriptions, \
    order_tables_by_topic_and_descriptions, \
    build_topic_keywords,\
    build_topic_icons,\
    found_column_position, \
    exclude_invisible_tables, \
    all_visible_tables,\
    order_queries_by_topics, \
    saved_queries_grouped_by_user_type,\
    saved_manual_requests_grouped_by_user_type,\
    saved_data_years, \
    build_foreign_keys, \
    run_queries_auth, \
    run_queries_anon,\
    located_in_area_value_to_column,\
    get_table_metadata_value,\
    build_aggregation_title,\
    save_value, \
    execute_query_on_django_db, \
    execute_query_on_main_db, \
    get_color, \
    exists_table, \
    table_visible, \
    get_table_description, \
    DESCRIPTION, \
    count_of_columns_table, \
    all_columns_have_metadata_description, \
    grouped_by_in_query, \
    stampa_symtobltabel, \
    SECONDARY, \
    groupedby_value_to_column, \
    column_with_same_description, \
    build_column_warnings_and_definitions, \
    build_table_external_medatata, \
    count_of_columns_no_obs_value, \
    get_client_ip, \
    SYNCHRONIZATION

from web.statistical_secret import apply_stat_secret, \
    detect_special_columns, \
    apply_stat_secret_plain, \
    headers_and_data, \
    store_data_frame
from web.topics import build_topics_decoder_dict, \
    filter_tables_by_topic, \
    build_topics_counter_dict, \
    get_topic_description, \
    get_topic_id,\
    build_topics_dict, \
    build_queries_to_topics_mapping
from web.actions import query_title,\
    query_description,\
    query_sql, \
    generate_report_action_csv, \
    generate_report_action_xls, \
    generate_report_action_xlsx, \
    generate_report_action_sdmx,\
    generate_report_action_rdf, \
    generate_report_action_turtle, \
    generate_report_action_json_stat,\
    generate_usage_report_action_xls
from datetime import datetime, date
from collections import OrderedDict
from utils import ALL
import json
import ast
import calendar
from explorer.models import MSG_FAILED_BLACKLIST
import subprocess
import shlex

def execute_query_viewmodel(request,
                            query,
                            aggregation,
                            pivot,
                            debug,
                            enable_manual_request,
                            form=None,
                            message=None):
    """
     A view model for the sql query; this is a django explorer customization.

    :param request: Django request.
    :param query: Explorer query.
    :param aggregation:
    :param pivot:
    :param debug:
    :param form:
    :param message:
    :param enable_manual_request:
    :return: The request response.
    """
    warn = None
    df = None
    store = None
    html = ""

    rows = url_get_rows(request)
    # I use this custom model in order to use my headers_and_data function
    # that apply the statistical secrets algorithm on the result set.
    st = detect_special_columns(query.sql)

    if len(st.cols) == 0:
        noj = _("No --JOIN statements specified in SQL")
        as_is = _("the table is shown as-is")
        no_stat = _("without preserving statistical secret")
        warn = "%s; %s %s." % (unicode(noj), unicode(as_is), unicode(no_stat))

    aggregations = get_aggregations(st.cols)

    if len(aggregation) == 0 and len(st.aggregation) > 0:
        aggregation = st.aggregation

    if len(aggregation) > 0:
        query.sql, error = build_aggregation_query(query.sql,
                                                   st.cols,
                                                   aggregation,
                                                   dict(),
                                                   st.threshold)
        st = detect_special_columns(query.sql)

    if st.include_descriptions:
        query.sql, h = build_description_query(query.sql,
                                               st.cols,
                                               [],
                                               False,
                                               False)

    old_head, data, duration, error = query.headers_and_data()
    if error is None:
        if len(old_head) < 3 and len(st.secret) + len(st.constraint) + len(
                st.secret_ref) == 1 and len(st.threshold) == 1:
            data, head, df = apply_stat_secret_plain(old_head,
                                                     data,
                                                     st.cols,
                                                     st.threshold,
                                                     st.constraint,
                                                     debug)
        # Check id I can give the full result set.
        elif (len(st.secret) + len(st.constraint) + len(st.secret_ref) != 0) \
                and (pivot is None or len(pivot) == 0):
            if len(st.pivot) > 0:
                pivot = st.pivot
            else:
                rs = _("Can not give you the result set")
                pr = _("in order to preserve the statistical secret")
                pl = _("please chose a pivot and push the Apply button")
                warn = "%s %s; %s." % (unicode(rs), unicode(pr), unicode(pl))
        else:
            data, head, df, warn_n, error = apply_stat_secret(old_head,
                                                              data,
                                                              st.cols,
                                                              pivot,
                                                              st.secret,
                                                              st.secret_ref,
                                                              st.threshold,
                                                              st.constraint,
                                                              dict(),
                                                              aggregation,
                                                              debug,
                                                              False,
                                                              False,
                                                              st.cols,
                                                              '',
                                                              False)
            if warn_n is not None and warn != "":
                warn = warn_n

        #print df

        if df is not None:
            store = store_data_frame(df)
            html = data_frame_to_html(df, True, pivot)

    return RequestContext(request,
                          {'store': store,
                           'warning': warn,
                           'error': error,
                           'params': query.available_params(),
                           'query': query,
                           'form': form,
                           'message': message,
                           'old_headers': old_head,
                           'dataframe': html,
                           'aggregate': aggregations,
                           'sel_aggregate': aggregation,
                           'pivot_column': pivot,
                           'rows': rows,
                           'threshold_columns': st.threshold,
                           'constr_columns': st.constraint,
                           'decoder_columns': st.decoder,
                           'debug': debug,
                           'manual_request': enable_manual_request,
                           'language_code': translation.get_language()})


def query_viewmodel_get(request,
                        query,
                        title=None,
                        form=None):
    """
     A view model for the sql query; this is a django explorer customization.
     This view does not execute the query.

    :param request: Django request.
    :param query: Explorer query.
    :param title: Title.
    :param form: Query form.
    :return: The request response.
    """
    return RequestContext(request,
                          {'params': query.available_params(),
                           'title': title,
                           'query': query,
                           'form': form})


class CreateQueryView(ExplorerContextMixin, CreateView):
    def dispatch(self, *args, **kwargs):
        return super(CreateQueryView, self).dispatch(*args, **kwargs)

    form_class = CreateQueryForm
    template_name = 'explorer/query.html'

    def post(self, request, *args, **kwargs):
        """
        Executed in create query view post request.

        :param request: Django request.
        :return: The request response.
        """
        save = request.REQUEST.get('save')
        to_be_saved = False
        if not save is None and save == 'true':
            to_be_saved = True

        pivot_column = request.REQUEST.get('pivot_column')
        pivot_cols = []
        if not pivot_column is None and pivot_column != "":
            pivot_cols = [ast.literal_eval(x) for x in pivot_column.split(',')]

        aggregation = request.REQUEST.get('aggregate')
        aggregation_ids = []
        if not aggregation is None and aggregation != "":
            aggregation_ids = [ast.literal_eval(x) for x in
                               aggregation.split(',')]

        debug = False
        if request.user.is_staff:
            debug_s = request.REQUEST.get('debug')
            if not debug_s is None and debug_s == 'true':
                debug = True

        form = CreateQueryForm(request.POST)
        message = None

        if to_be_saved:
            try:
                model_instance = form.save(commit=False)
                change_query(model_instance, pivot_cols, aggregation_ids)
                form.save()
            except ValueError:
                message = _("The Query could not be saved because "
                            "the data didn't validate.")
        else:
            try:
                form.save(commit=False)
                vm = execute_query_viewmodel(request,
                                             form.instance,
                                             aggregation_ids,
                                             pivot_cols,
                                             debug,
                                             False,
                                             form=form,
                                             message=message)
                return self.render_template('explorer/query.html', vm)
            except ValueError:
                message = _("The Query could not be executed because "
                            "the data didn't validate.")

        query = form.instance
        if message is not None:
            vm = RequestContext(request,
                                {'message': message,
                                 'params': query.available_params(),
                                 'query': query,
                                 'form': form,
                                 'debug': debug,
                                 'manual_request': False,
                                 'language_code': translation.get_language()})
            return self.render_template('explorer/query.html', vm)

        url = '/explorer/%d/' % query.pk
        return redirect(url)


class QueryView(ExplorerContextMixin, View):
    """
    A query view that uses a custom QueryForm with custom validation.
    """
    @method_decorator(view_permission)
    def dispatch(self, *args, **kwargs):
        """
        Dispatch the method of the superclass.

        :param args:
        :param kwargs:
        :return:
        """
        return super(QueryView, self).dispatch(*args, **kwargs)

    def get(self, request, query_id):
        """
        Executed in query view get request.

        :param request: Django request.
        :param query_id: Query id.
        :return: The request response.
        """

        query, form = QueryView.get_instance_and_form(request, query_id)
        if QueryView.user_has_privilege(request.user, query):
            query.save()  # save query and updates the modified date also.
        variable_dictionary, error_msg = get_variable_dictionary(query)
        types = get_types_dictionary(variable_dictionary)
        widgets = get_widgets_dictionary(variable_dictionary)
        vm = query_viewmodel_get(request, query, form=form)
        vm['types'] = types
        vm['widgets'] = widgets
        vm['manual_request'] = True

        url = '/explorer/%d/' % query.pk
        vm['title'] = query.title
        vm['url'] = url
        return self.render_template('explorer/query.html', vm)

    def post(self, request, query_id):
        """
        Executed in query view post request.

        :param request: Django request.
        :param query_id: Query id.
        :return: The request response.
        """

        save = request.REQUEST.get('save')
        to_be_saved = False
        if not save is None and save == 'true':
            to_be_saved = True

        aggregation = request.REQUEST.get('aggregate')
        aggregation_ids = []
        if not aggregation is None and aggregation != "":
            aggregation_ids = [ast.literal_eval(x) for x in
                               aggregation.split(',')]

        debug = False
        if request.user.is_staff:
            debug_s = request.REQUEST.get('debug')
            if not debug_s is None and debug_s == 'true':
                debug = True

        pivot_column = request.REQUEST.get('pivot_column')
        pivot_cols = []
        if not pivot_column is None and pivot_column != "":
            pivot_cols = [ast.literal_eval(x) for x in pivot_column.split(',')]

        query = get_object_or_404(Query, pk=query_id)
        url = '/explorer/%d/' % query.pk
        message = None
        form = CreateQueryForm(data=request.POST or None, instance=query)
        if to_be_saved and QueryView.user_has_privilege(request.user,
                                                        query):
            try:
                model_instance = form.save(commit=False)
                change_query(model_instance, pivot_cols, aggregation_ids)
                form.save()
            except ValueError:
                message = _("The Query could not be saved because "
                            "the data didn't validate.")
        else:
            try:
                query, form = QueryView.get_instance_and_form(request,
                                                              query_id)
                form.save(commit=False)
                vm = execute_query_viewmodel(request,
                                             form.instance,
                                             aggregation_ids,
                                             pivot_cols,
                                             debug,
                                             True,
                                             form=form,
                                             message=message)
                variable_dictionary, message = get_variable_dictionary(query)
                types = get_types_dictionary(variable_dictionary)
                widgets = get_widgets_dictionary(variable_dictionary)
                vm['types'] = types
                vm['widgets'] = widgets
                vm['title'] = query.title
                vm['url'] = url
                return self.render_template('explorer/query.html', vm)
            except ValueError, e:
                message = _("The Query could not be executed because "
                            "the data didn't validate.")
                #@TODO Handle better exception message and remove print.
                print e.message
                import traceback
                print traceback.format_exc()

        if message is not None:
            vm = RequestContext(request,
                                {'message': message,
                                 'params': query.available_params(),
                                 'query': query,
                                 'form': form,
                                 'debug': debug,
                                 'manual_request': True,
                                 'language_code': translation.get_language()})
            vm['title'] = query.title
            vm['url'] = url
            return self.render_template('explorer/query.html', vm)

        return redirect(url)

    @staticmethod
    def user_has_privilege(user, query):
        """
        Has the user the privilege to modify the query?

        :param user: Django user.
        :param query: Explore query.
        :return:
        """
        if user.is_authenticated() and user.email == query.created_by:
            return True
        return False

    @staticmethod
    def get_instance_and_form(request, query_id):
        """
        Get query instance and query form.

        :param request: Django request.
        :param query_id: Query id.
        :return: query,form
        """
        query = get_object_or_404(Query, pk=query_id)
        variable_dictionary, error_msg = get_variable_dictionary(query)
        query.params = url_get_parameters(request)

        # The url with params is not been specified.
        if query.params is None:
            # Parameters taken by dictionary
            query.params = get_params_dictionary(variable_dictionary)

        form = QueryForm(request.POST if len(request.POST) else None,
                         instance=query,
                         variable_dictionary=variable_dictionary,
                         error_msg=error_msg)

        return query, form


def query_download_csv(request):
    """
    Download query in CSV format.

    :param request: Django request.
    :return: The request response.
    """
    fn = generate_report_action_csv(request)
    title = query_title(request)
    return fn(title)


def query_download_xls(request):
    """
    Download query in Excel 97 format.

    :param request: Django request.
    :return: The request response.
    """
    fn = generate_report_action_xls(request)
    title = query_title(request)
    description = query_description(request)

    return fn(title, description)


def query_download_xlsx(request):
    """
    Download query in Excel xlsx 2007 format.

    :param request: Django request.
    :return: The request response.
    """
    fn = generate_report_action_xlsx(request)
    title = query_title(request)
    description = query_description(request)
    return fn(title, description)


def query_download_json_stat(request):
    """
    Download query in Json stat format.

    :param request: Django request.
    :return: The request response.
    """
    title = query_title(request)
    fn = generate_report_action_json_stat(request)
    return fn(title)


def query_download_sdmx(request):
    """
    Download query in Sdmx xlm message format.

    :param request: Django request.
    :return: The request response.
    """
    title = query_title(request)
    fn = generate_report_action_sdmx(request)
    sql = query_sql(request)
    return fn(title, sql)


def query_download_rdf(request):
    """
    Download query in Rdf data cube format.

    :param request: Django request.
    :return: The request response.
    """
    title = query_title(request)
    sql = query_sql(request)
    description = query_description(request)
    fn = generate_report_action_rdf(request)
    return fn(title, description, sql)


def query_download_turtle(request):
    """
    Download query in Rdf data cube format.

    :param request: Django request.
    :return: The request response.
    """
    title = query_title(request)
    sql = query_sql(request)
    description = query_description(request)
    fn = generate_report_action_turtle(request)
    return fn(title, description, sql)


@user_passes_test(lambda u: u.is_staff)
def empty_table(request):
    """
    Delete the table passed by url.

    :param request: Django request.
    :return: The request response.
    """
    table_name = request.GET.get('table')
    if not table_name is None and table_name != "":
        delete_all(table_name)
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


@login_required()
def delete_query(request, pk):
    """
    Delete query with requested pk.

    :param request: Django request.
    :param pk: The query public key.
    :return: The request response.
    """
    query = Query.objects.get(pk=pk)
    if request.user.email == query.created_by:
        query.delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


@user_passes_test(lambda u: u.is_staff)
def test_table(request):
    """
    View in order to add random values to a test table.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    if request.method == 'POST':
        form = TestForm(request.POST)
        if form.is_valid():
            table_name = form.cleaned_data['table_name']
            rows = form.cleaned_data['rows']
            min_value = form.cleaned_data['min_value']
            max_value = form.cleaned_data['max_value']
            insert_random_data(table_name, rows, min_value, max_value)
            return HttpResponseRedirect(request.META.get('HTTP_REFERER'))
        else:
            return render_to_response('l4s/test_table.html',
                                      {'form': form},
                                      context_instance=context)

    form = TestForm(initial={'table_name': 'web_test3',
                             'rows': 10,
                             'min_value': 1,
                             'max_value': 100})
    return render_to_response('l4s/test_table.html',
                              {'form': form},
                              context_instance=context)


@user_passes_test(lambda u: u.is_staff)
def table_add_metadata(request):
    """
    Add table-column metadata.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    if request.method == 'POST':
        form = MetadataForm(request.POST)
        if form.is_valid():
            form.save()
            table_name = form.cleaned_data['table_name']
            column_name = form.cleaned_data['column_name']
            url = '/table/metadata/?table=' + \
                  table_name + ';column=' + \
                  column_name
            return redirect(url)
        else:
            return render_to_response('l4s/metadata_add.html',
                                      {'form': form},
                                      context_instance=context)

    table_name = request.GET.get('table')
    column_name = request.GET.get('column')
    form = MetadataForm(initial={'table_name': table_name,
                                 'column_name': column_name})
    context['table_name'] = table_name
    context['column_name'] = column_name
    return render_to_response('l4s/metadata_add.html',
                              {'form': form},
                              context_instance=context)


@user_passes_test(lambda u: u.is_staff)
def table_delete_metadata(request):
    """
    Delete table-column metadata.

    :param request: Django request.
    :return: The request response.
    """
    metadata_id = request.GET.get('id')
    if not metadata_id is None and metadata_id != "":
        Metadata.objects.filter(id=metadata_id).delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


@user_passes_test(lambda u: u.is_staff)
def table_edit_metadata(request):
    """
    Edit table-column metadata.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    metadata_id = request.REQUEST.get('id')
    metadata = Metadata.objects.get(id=metadata_id)
    if request.method == 'POST':
        form = MetadataChangeForm(request.POST, instance=metadata)
        if form.is_valid():
            form.save()
            table_name = form.cleaned_data['table_name']
            column_name = form.cleaned_data['column_name']
            url = '/table/metadata/?table=' + \
                  table_name + ';column=' + \
                  column_name
            return redirect(url)
        else:
            return render_to_response('l4s/metadata_edit.html',
                                      {'form': form},
                                      context_instance=context)

    form = MetadataChangeForm(instance=metadata)
    context['id'] = metadata_id
    context['table_name'] = metadata.table_name
    context['column_name'] = metadata.column_name

    return render_to_response('l4s/metadata_edit.html',
                              {'form': form},
                              context_instance=context)


@user_passes_test(lambda u: u.is_staff)
def table_view_metadata(request):
    """
    View metadata on table.

    :param request: Django request.
    :return: T
    he Django request response.
    """

    context = RequestContext(request)
    table_name = request.GET.get('table')
    column_name = request.GET.get('column', '')
    if not table_name is None and table_name != "":
        context['table_name'] = table_name

    if not column_name is None and column_name != "":
        metadata_list = Metadata.objects.filter(table_name=table_name,
                                                column_name=column_name)
        context['column_name'] = column_name
        aggregations = located_in_area_value_to_column(metadata_list)
        groupedby = groupedby_value_to_column(metadata_list)
    else:
        aggregations = dict()
        groupedby = dict()
        metadata_list = Metadata.objects.filter(table_name=table_name)

    context['metadata_list'] = metadata_list
    context['aggregations'] = aggregations
    context['groupedby'] = groupedby
    context['request'] = request
    return render_to_response('l4s/metadata.html', context)


@user_passes_test(lambda u: u.is_staff)
def delete_ontology(request):
    """
    Delete ontology.

    :param request: Django request.
    :return: The Django request response.
    """
    ontology_id = request.GET.get('id')
    if not ontology_id is None and ontology_id != "":
        item = OntologyFileModel.objects.get(id=ontology_id)
        item.delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


@user_passes_test(lambda u: u.is_superuser)
def add_ontology(request):
    """
    Upload file.

    :param request: Django request.
    :return: The Django request response.
    """
    if request.method == 'POST':
        form = OntologyFileForm(request.POST, request.FILES)
        if form.is_valid():
            upload = OntologyFileModel(upload=request.FILES['upload'])
            upload.name = request.FILES['upload'].name
            upload.save()
            return HttpResponseRedirect('/ontology')
        else:
            return render_to_response('l4s/add_ontology.html',
                                      {'form': form},
                                      context_instance=RequestContext(request))

    form = OntologyFileForm()
    return render_to_response('l4s/add_ontology.html',
                              {'form': form},
                              context_instance=RequestContext(request))


def no_implemented(request):
    """
    Render a page that say that the feature is not implemented yet.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    return render_to_response('l4s/no_implemented.html', context)


@user_passes_test(lambda u: u.is_superuser)
def ontology(request):
    """
    View ontologies loaded in the system

    :param request: Django request.
    :return: The Django request response.
    """
    ontology_list = OntologyFileModel.objects.all()
    context = Context({'ontology_list': ontology_list})
    context['request'] = request
    return render_to_response('l4s/ontology.html',
                              context_instance=context)


@user_passes_test(lambda u: u.is_superuser)
def source_table_list(request):
    """
    List the tables in the source database.

    :param request: Django request.
    :return: The Django request response.
    """
    connection = connections['source']
    tables = connection.introspection.table_names()
    table_description_dict = build_description_table_dict(tables)

    context = Context({'table_list': tables})
    context['request'] = request
    context['table_description_dict'] = table_description_dict
    context['topics'] = build_topics_decoder_dict()
    return render_to_response("l4s/source_table_list.html", context)


@user_passes_test(lambda u: u.is_staff)
def usage_report_download_xls(request):
    fn = generate_usage_report_action_xls(request)
    return fn()


@user_passes_test(lambda u: u.is_staff)
def usage_report(request):
    """
    View usage report.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    year_s = request.GET.get('year')
    if not year_s is None:
        sel_year = ast.literal_eval(year_s)
    else:
        sel_year = date.today().year

    month_s = request.GET.get('month')
    if not month_s is None:
        sel_month = ast.literal_eval(month_s)
    else:
        sel_month = None

    years = saved_data_years()
    months = [[m, mon.title()] for m, mon in enumerate(calendar.month_name)]
    months[0][0] = None
    months[0][1] = ALL

    if sel_month is None:
        selected_month_name = ALL
    else:
        selected_month_name = months[sel_month][1]

    queries = saved_queries_grouped_by_user_type(sel_year, sel_month)
    manual_requests = saved_manual_requests_grouped_by_user_type(sel_year,
                                                                 sel_month)
    run_queries_au = run_queries_auth(sel_year, sel_month)
    run_queries_an = run_queries_anon(sel_year, sel_month)

    context['queries'] = queries
    context['manual_requests'] = manual_requests
    context['run_queries_auth'] = run_queries_au
    context['run_queries_anon'] = run_queries_an
    context['years'] = years
    context['months'] = months
    context['selected_year'] = sel_year
    context['selected_month'] = sel_month
    context['selected_month_name'] = selected_month_name
    return render_to_response("l4s/usage_report.html", context)


@user_passes_test(lambda u: u.is_staff)
def table_list(request):
    """
    List the tables in the main database.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    topic = request.GET.get('topic')
    search = request.GET.get('search', '')

    table_description = dict()
    # Filter tables matching descriptions and table_name.
    if search:
        table_description = get_table_by_name_or_desc(search, None,
                                                      'table_name')
        tables = table_description.keys()
    else:
        connection = connections[EXPLORER_CONNECTION_NAME]
        tables = connection.introspection.table_names()

    if not topic is None and topic != "":
        topic_id = ast.literal_eval(topic)
    else:
        # Topic 0 means that all the topics will be displayed.
        topic_id = 0

    if topic_id != 0:
        tables = filter_tables_by_topic(topic_id, tables, 'nome')

    if not search:
        table_description = build_description_table_dict(tables)

    context['topics'] = build_topics_decoder_dict()
    context['table_list'] = tables
    context['search'] = search
    context['table_description'] = table_description
    context['selected_topic'] = topic_id

    return render_to_response("l4s/table_list.html", context)


@user_passes_test(lambda u: u.is_staff)
def table(request):
    """
    Request to view a table structure.

    :param request: Django request.
    :return: The Django request response.
    """

    table_name = request.GET.get('name', '')

    #print exists_table('public', table_name)

    if exists_table('public', table_name) == True:

        table_schema = get_table_schema(table_name)

        column_description = build_description_column_dict(table_name,
                                                           table_schema,
                                                           False)

        fks = build_foreign_keys(table_name)
        context = Context({'table_schema': table_schema})
        context['table_name'] = table_name
        context['request'] = request
        context['column_description'] = column_description
        context['fks'] = fks
        return render_to_response("l4s/table.html", context)

    else:
        context = Context({})
        context['error_string'] = _("The table does not exist")
        send_mail('Errore Lod4Stat (' +
                  str(request.user) + ') ' +
                  ''.join(ALLOWED_HOSTS) +
                  '/table/?name=' + table_name,
                  unicode(context['error_string']), DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL, fail_silently=False)
        return render_to_response("l4s/error.html", context)



    #print fks



def open_data(request):
    """
    List of open data to be downloaded plain.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    objects = Query.objects.filter(open_data='true')
    context['object_list'] = objects
    return render_to_response("l4s/open_data.html", context)


def query_list(request):
    """
    List the queries using this custom view.
    This is used for logged users only.

    :param request: Django request.
    :return: The Django request response.
    """

    search = request.GET.get('search', '')
    public = request.GET.get('public', '')
    topic = request.GET.get('topic')

    if not topic is None and topic != "":
        selected_topic = ast.literal_eval(topic)
    else:
        # Topic 0 means that all the topics will be displayed.
        selected_topic = 0

    queries = (Query.objects.filter(created_by=request.user) |
               Query.objects.filter(is_public='true'))

    """
    print queries
    print "search ", search
    print "public " , public
    print "topic " , topic
    """

    if search:
        queries = queries & (Query.objects.filter(title__icontains=search) |
                             Query.objects.filter(
                                 description__icontains=search))

    queries_to_topics = build_queries_to_topics_mapping(queries, selected_topic)

    order_by = request.GET.get('order_by')

    #print "order_by " , order_by
    #print "queries_to_topics ", queries_to_topics

    if order_by is None or order_by == "topic":
        #print "aaaaa"
        queries = order_queries_by_topics(queries_to_topics.items())
    else:
        queries = queries.order_by(order_by)

    topic_mapping = build_topics_decoder_dict()
    icons = build_topic_icons()

    context = Context({'queries': queries})
    context['selected_topic'] = selected_topic
    context['icons'] = icons
    context['request'] = request
    context['order_by'] = order_by
    context['public'] = public
    context['tables'] = queries_to_topics
    context['topics'] = topic_mapping

    #print queries

    return render_to_response("explorer/query_list.html", context)


@user_passes_test(lambda u: u.is_staff)
def recent_queries(request):
    """
    List the recent queries.
    This is used for internal users only.

    :param request: Django request.
    :return: The Django request response.
    """
    search = request.GET.get('search')
    objects = Query.objects
    if not search is None and search != "":
        objects = objects & (Query.objects.filter(title__icontains=search) |
                             Query.objects.filter(description__icontains=search))

    recent = objects.order_by('-last_run_date')
    recent = recent[:EXPLORER_RECENT_QUERY_COUNT]
    context = Context({'object_list': objects})
    context['request'] = request
    context['recent_queries'] = recent
    return render_to_response("explorer/recent_queries.html", context)


@login_required()
def query_copy(request):
    """
    Copy public query in the query of logged  user.

    :param request: Django request.
    :return: The Django request response.
    """
    query_id = request.GET.get('id')
    if not query_id is None and query_id != "":
        query = get_object_or_404(Query, id=query_id)
        new_query = deepcopy(query)
        new_query.id = None
        new_query.is_public = False
        # The author of the new copied query is the authenticated user.
        new_query.created_by = request.user
        new_query.save()
    url = '/explorer'
    return redirect(url)


def index(request):
    """
    Show index the first public page shown to the not logged user.

    :param request: Django request.
    :return: The Django request response.
    """
    search = request.GET.get('search')
    objects = Query.objects.filter(is_public=True)
    if not search is None and search != "":
        found = Query.objects.filter(title__icontains=search)
        found = found | Query.objects.filter(description__icontains=search)
        objects = objects & found

    context = RequestContext(request)
    context['object_list'] = objects
    return render_to_response("l4s/index_new.html", context)


def legal_notes(request):
    """
    Show legal notes.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    return render_to_response("l4s/legal_notes.html", context)


def privacy_policy(request):
    """
    Show privacy policy.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    return render_to_response("l4s/privacy_policy.html", context)


def query_editor_external_metadata(request):

    context = RequestContext(request)

    table_name = request.REQUEST.get('table')

    column_warnings = build_column_warnings_and_definitions(table_name, True, False)
    column_definitions = build_column_warnings_and_definitions(table_name, False, False)
    table_external_metadata = build_table_external_medatata(table_name)

    """
    print "column_warnings", column_warnings
    print "column_definitions", column_definitions
    print "table_external_metadata", table_external_metadata
    """

    context['column_warnings'] = column_warnings
    context['column_definitions'] = column_definitions
    context['table_external_metadata'] = table_external_metadata

    context['tipo'] = request.REQUEST.get('tipo')

    return render_to_response("l4s/query_editor_external_metadata.html", context)

def query_editor_customize(request):
    """
    Customize the default query in editor.

    :param request: Django request.
    :return: The Django request response.
    """

    context = RequestContext(request)
    debug = False
    if request.user.is_staff:
        debug_s = request.REQUEST.get('debug')
        if not debug_s is None and debug_s == 'true':
            debug = True

    include_code = False
    include_code_s = request.REQUEST.get('include_code')
    if not include_code_s is None and include_code_s == 'true':
        include_code = True

    range = False
    if request.user.is_staff:
        range_s = request.REQUEST.get('range')
        if not range_s is None and range_s == 'true':
            range = True

    selected_obs_values_s = request.REQUEST.get('selected_obs_values')
    selected_obs_values = []
    if not selected_obs_values_s is None and selected_obs_values != "":
        selected_obs_values = [x for x in selected_obs_values_s.split(',')]

    table_name = request.REQUEST.get('table')
    table_schema = get_table_schema(table_name)

    columns = request.REQUEST.get('columns')
    cols = OrderedDict()
    if not columns is None and columns != "":
        for x in columns.split(','):
            cols[found_column_position(x, table_schema)] = x

    rows_s = request.REQUEST.get('rows')
    rows = OrderedDict()
    if not rows_s is None and rows_s != "":
        for x in rows_s.split(','):
            rows[found_column_position(x, table_schema)] = x

    ref_periods = request.REQUEST.get('ref_periods')
    periods = []
    if not ref_periods is None and ref_periods != "":
        periods = [x for x in ref_periods.split(',')]

    sel_aggregation = request.REQUEST.get('aggregate')
    not_sel_aggregations = request.REQUEST.get('not_sel_aggregations')

    not_sel_aggregation_ids = []
    if not_sel_aggregations is not None and not_sel_aggregations != "":
        not_sel_aggregation_ids = [x for x in not_sel_aggregations.split(',')]

    sel_aggregation_ids = []
    if sel_aggregation is not None and sel_aggregation != "":
        sel_aggregation_ids = [x for x in sel_aggregation.split(',')]

    column_description = request.REQUEST.get('column_description')
    grouped_by_in_query = request.REQUEST.get('grouped_by_in_query')

    hidden_fields = request.REQUEST.get('hidden_fields')

    fields = [field.name for field in table_schema]

    obs_values = all_obs_value_column(table_name, table_schema, json.loads(hidden_fields)).values()

    context['fields'] = fields
    context['obs_values'] = obs_values
    context['selected_obs_values'] = selected_obs_values
    context['table_name'] = table_name
    context['columns'] = cols
    context['rows'] = rows
    context['include_code'] = include_code
    context['range'] = range

    values = request.REQUEST.get('values')
    agg_values = request.REQUEST.get('agg_values')
    not_agg_selection_value = request.REQUEST.get('not_agg_selection_value')
    aggregations = request.REQUEST.get('aggregations')


    filters = request.REQUEST.get('filters')

    agg_filters = request.REQUEST.get('agg_filters')

    """
    print "agg_values ", agg_values
    print "filters ", filters
    print "values ", values


    print "sel_aggregation ", sel_aggregation_ids
    print "agg_filters ", agg_filters


    print "not_sel_aggregations ", not_sel_aggregation_ids
    print "not_agg_selection_value ", not_agg_selection_value
    """

    context['aggregations'] = json.loads(aggregations)
    context['sel_aggregation'] = sel_aggregation_ids
    context['column_description'] = json.loads(column_description)
    context['grouped_by_in_query'] = json.loads(grouped_by_in_query)
    context['debug'] = debug
    context['values'] = json.loads(values)
    context['agg_values'] = json.loads(agg_values)
    context['filters'] = json.loads(filters)
    context['agg_filters'] = json.loads(agg_filters)
    context['hidden_fields'] = json.loads(hidden_fields)

    context['not_sel_aggregations'] = not_sel_aggregation_ids
    context['not_agg_selection_value'] = json.loads(not_agg_selection_value)

    context['ref_periods'] = periods

    """
    print "fields ", fields
    print "obs_values ", obs_values
    print "columns ", cols
    print "rows ", rows
    print "hidden_fields ", hidden_fields
    print "aggregations ", aggregations
    print "column_description ", column_description
    print "grouped_by_in_query", grouped_by_in_query
    """

    sec = get_table_metadata_value(table_name, SECONDARY)

    if not sec is None and len(sec) > 0:
        context['secondary'] = True
    else:
        context['secondary'] = False

    return render_to_response("l4s/query_editor_customize.html", context)


def query_editor_view(request):
    """
    Query editor view.

    :param request: Django request.
    :return: The request response.
    """

    #save_value('manuel', 'fino qui arrivo')

    #print datetime.now().strftime("%H:%M:%S.%f")

    table_name = request.REQUEST.get('table')
    context = RequestContext(request)
    context['error_string'] = ''

    if exists_table('public', table_name) == False:
        context['error_string'] = _("The table does not exist")

    if context['error_string'] == '':
        if table_visible(table_name) == False:
            context['error_string'] = _("The table are not visible")

    if context['error_string'] == '':
        if count_of_columns_no_obs_value('public', table_name) < 2:
            context['error_string'] = _("The table must have at least 2 columns not obs_value")

    if context['error_string'] == '':
        if get_table_description(table_name) == None:
            error = unicode(_("Please add the metadata "))
            error += " " + unicode(_("with key")) + " '" + DESCRIPTION + "' "
            error += unicode(_("for table"))
            error += " '" + table_name + "'"
            context['error_string'] = error

    if context['error_string'] == '':
        if all_columns_have_metadata_description('public', table_name) == False:
            context['error_string'] = _("All columns mast have description metadata")

    if context['error_string'] == '':
        if column_with_same_description('public', table_name) == True:
            context['error_string'] = _("There are one ore plus fields on table %s with same description metadata") % table_name.upper()

    if context['error_string'] != '':
        send_mail('Errore Lod4Stat (' +
                  str(request.user) + ') ' +
                  ''.join(ALLOWED_HOSTS) +
                  '/query_editor_view/?table=' + table_name,
                  unicode(context['error_string']), DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL, fail_silently=False)
        return render_to_response("l4s/error.html", context)

    topic = get_topic_description(table_name)
    topic_id = get_topic_id(table_name)
    context['table_name'] = table_name
    table_description_dict = build_description_table_dict([table_name])
    context['table_description'] = table_description_dict[table_name]

    selected_obs_values_s = request.REQUEST.get('selected_obs_values')
    selected_obs_values = []
    if not selected_obs_values_s is None and selected_obs_values != "":
        selected_obs_values = [x for x in selected_obs_values_s.split(',')]

    filters_s = request.REQUEST.get('filters')
    agg_filters_s = request.REQUEST.get('agg_filters')
    not_agg_selection_value_s = request.REQUEST.get('not_agg_selection_value')

    columns = request.REQUEST.get('columns')
    cols = []
    if not columns is None and columns != "":
        cols = [x for x in columns.split(',')]

    rows_s = request.REQUEST.get('rows')
    rows = []
    if not rows_s is None and rows_s != "":
        rows = [x for x in rows_s.split(',')]

    debug = False
    visible = False
    if request.user.is_staff:
        debug_s = request.REQUEST.get('debug')
        if not debug_s is None and debug_s == 'true':
            debug = True
        visible_s = request.REQUEST.get('visible')
        if not visible_s is None and visible_s == 'true':
            visible = True

    include_code = False
    include_code_s = request.REQUEST.get('include_code')
    if not include_code_s is None and include_code_s == 'true':
        include_code = True

    range = False
    range_s = request.REQUEST.get('range')
    if not range_s is None and range_s == 'true':
        range = True

    aggregation = request.REQUEST.get('aggregate', "")
    not_sel_aggregations = request.REQUEST.get('not_sel_aggregations', "")

    #print "aggregation " ,aggregation

    not_sel_aggregations_ids = []
    if not_sel_aggregations is not None and not_sel_aggregations != "":
        not_sel_aggregations_ids = [ast.literal_eval(x) for x in not_sel_aggregations.split(',')]

    aggregation_ids = []
    if aggregation is not None and aggregation != "":
        aggregation_ids = [ast.literal_eval(x) for x in aggregation.split(',')]

    #print "dd ", datetime.now().strftime("%H:%M:%S.%f")

    values = dict()
    table_schema = get_table_schema(table_name)

    hidden_fields = all_hidden_fields(table_name, table_schema)

    #print hidden_fields

    #print "dd1 ", datetime.now().strftime("%H:%M:%S.%f")

    obs_values = all_obs_value_column(table_name, table_schema, hidden_fields).values()

    #print obs_values
    #print type(obs_values)

    if len(selected_obs_values) == 0:
        # Take all.
        selected_obs_values = obs_values

    #print "table_schema ", table_schema
    #print "dd2 ", datetime.now().strftime("%H:%M:%S.%f")

    for f, field in enumerate(table_schema):
        column_name = field.name
        if not column_name in obs_values:
            try:
                #print "xxx ", datetime.now().strftime("%H:%M:%S.%f")
                vals = get_all_field_values(table_name, column_name, None)
            except MissingMetadataException, e:
                context['error'] = "%s" % (unicode(e.message))
                send_mail('Errore Lod4Stat (' +
                          str(request.user) + ') ' +
                          ''.join(ALLOWED_HOSTS) +
                          '/query_editor_view/?table=' +
                          ''.join(request.GET.getlist("table")), unicode(e.message), DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL, fail_silently=False)
                return render_to_response("l4s/query_editor_view.html", context)
            values[column_name] = vals

    #print "dd3 ", datetime.now().strftime("%H:%M:%S.%f")

    ref_periods = list_ref_period(table_name, table_schema)

    #print "dd4 ", datetime.now().strftime("%H:%M:%S.%f")

    """
    print "table_name ", table_name
    print "table_schema ", table_schema
    print "ref_periods ", ref_periods
    """

    #print "request.GET", request.GET

    try:
      aggregations, agg_values = get_all_aggregations(table_name)
    except MissingMetadataException, e:
        context['error'] = "%s" % (unicode(e.message))
        send_mail('Errore Lod4Stat (' +
                  str(request.user) + ') ' +
                  ''.join(ALLOWED_HOSTS) +
                  '/query_editor_view/?table=' +
                  ''.join(request.GET.getlist("table")), unicode(e.message), DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL, fail_silently=False)
        return render_to_response("l4s/query_editor_view.html", context)

    """
    print "--------------------------------------"
    print "aggregations " , aggregations
    print "agg_values ", agg_values
    print len(rows)
    print len(cols)
    print "--------------------------------------"
    """

    #print table_name

    filters = dict()
    if len(rows) + len(cols) == 0:
        try:
            cols, rows = choose_default_axis(table_name,
                                             ref_periods,
                                             hidden_fields)
        except MissingMetadataException, e:
            context['error'] = "%s" % (unicode(e.message))
            send_mail('Errore Lod4Stat (' +
                      str(request.user) + ') ' +
                      ''.join(ALLOWED_HOSTS) +
                      '/query_editor_view/?table=' +
                      ''.join(request.GET.getlist("table")), unicode(e.message), DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL, fail_silently=False)
            return render_to_response("l4s/query_editor_view.html", context)

        if len(cols) > 1:
            if cols[0] in ref_periods.values():
                for val in values:
                    if val == cols[0]:
                        filters[val] = [values[val][len(values[val])-1]]
                    else:
                        filters[val] = values[val]
        else:
            filters = values

        #agg_filters = agg_values #boh ... non serve
        agg_filters = dict()
        #not_agg_selection_value = agg_values #boh ... non serve
        not_agg_selection_value = dict()
    else:
        filters = json.loads(filters_s)
        agg_filters = json.loads(agg_filters_s)
        not_agg_selection_value = json.loads(not_agg_selection_value_s)


    column_description = build_description_column_dict(table_name,
                                                       table_schema,
                                                       True)

    #print column_description

    if request.REQUEST.get('get_grouped_by_value') == None: #se non sono ancora passato dal personalizza prendo il default
        context['grouped_by_in_query'] = grouped_by_in_query(request.user, table_name, column_description)
    else:

        result = dict()

        for index1 in column_description:

            value = dict()

            for index2 in json.loads(request.REQUEST.get('get_grouped_by_value')):

                if column_description[index1]['table_name'] == index2["table_name"]:
                    if column_description[index1]['name'] == index2["column_name"]:

                        value['table_name'] = index2["table_name"]
                        value['column_name'] = index2["column_name"]
                        value['valore'] = index2["valore"]

            result[index1] = value

        #print result

        context['grouped_by_in_query'] = result


    #print "grouped_by_in_query" , context['grouped_by_in_query']

    #print "get_grouped_by_value ", get_grouped_by_value

    #print "ee ", datetime.now().strftime("%H:%M:%S.%f")

    """
    print "filters ", filters
    print "agg_filters ", agg_filters
    print "values ", values
    """

    """
    print "not_sel_aggregations " , not_sel_aggregations
    print "not_agg_selection_value " , not_agg_selection_value
    print "not_sel_aggregations_ids ", not_sel_aggregations_ids
    """

    #pivot indica i field che sono messi in colonna
    #sql e' l'sql da eseguire

    sql, pivot = build_query(table_name,
                             cols,
                             rows,
                             selected_obs_values,
                             aggregation_ids,
                             filters,
                             values,
                             not_agg_selection_value,
                             context['grouped_by_in_query'],
                             table_schema)

    #print "sql", sql
    #print "pivot", pivot

    query = Query(title=table_name, sql=sql)

    df, data, warn, err = headers_and_data(request.user,
                                           query,
                                           filters,
                                           aggregation_ids,
                                           agg_filters,
                                           pivot,
                                           debug,
                                           True,
                                           include_code,
                                           visible,
                                           range,
                                           get_client_ip(request),
                                           table_name,
                                           table_schema)

    #print datetime.now().strftime("%H:%M:%S.%f")

    context['values'] = values
    context['obs_values'] = obs_values
    context['selected_obs_values'] = ",".join(selected_obs_values)
    context['aggregations'] = aggregations # tutte le aggregazioni possibili per quella tabella ... serve per renderizzare poi il personalizza
    context['agg_values'] = agg_values #tutti i valori delle aggregazioni per quella tabella ... serve per renderizzare poi il personalizza
    #context['aggregation_ids'] = aggregation_ids
    context['topic'] = topic
    context['topic_id'] = topic_id
    context['hidden_fields'] = hidden_fields
    context['columns'] = ",".join(cols)
    context['rows'] = ",".join(rows)
    context['filters'] = filters

    context['agg_filters'] = agg_filters
    context['sel_aggregate'] = aggregation

    context['debug'] = debug
    context['include_code'] = include_code
    context['range'] = range
    context['ref_periods'] = ",".join(ref_periods.values())
    context['legend'] = LEGEND
    context['dl_art'] = DL_ART

    context['not_sel_aggregations'] = not_sel_aggregations
    context['not_agg_selection_value'] = not_agg_selection_value

    st = detect_special_columns(query.sql)

    context['show_legend'] = len(st.secret) > 0

    #print len(st.secret)

    #print "agg_values " , agg_values

    #print "dddddddddddddd"

    #print "column_description ", column_description

    #print "eeeeeee"

    #print "aggregations" , aggregations
    #print "agg_values " , agg_values

    agg_col, sel_tab = build_query_summary(column_description,  #crea gli elementi per poi disegnare a video la tabellina riassuntiva
                                           values,
                                           filters,
                                           aggregation_ids,
                                           aggregations,
                                           agg_values,
                                           agg_filters,
                                           cols,
                                           rows,
                                           hidden_fields,
                                           not_sel_aggregations_ids,
                                           not_agg_selection_value)

    """
    print "column_description" , column_description
    print "agg_col", agg_col
    print "cols", cols
    print "rows", rows
    """

    #print "agg_col", agg_col
    #print "sel_tab" , sel_tab

    column_value_external_metadata = build_column_warnings_and_definitions(table_name, True, True)

    #print get_color()
    #print "column_value_external_metadata", column_value_external_metadata

    title = build_query_title(column_description,
                              selected_obs_values,
                              agg_col,
                              cols,
                              rows)
    context['title'] = title

    description = build_query_desc(agg_col, sel_tab)
    context['description'] = description
    context['sel_tab'] = sel_tab
    context['agg_col'] = agg_col
    context['column_description'] = column_description

    #print "df", df

    if df is None:

        if (err == MSG_FAILED_BLACKLIST):
            elementi_request = ''
            no_display = _("Table name can not contain DELETE, INSERT o UPDATE string")
        else:
            if selected_obs_values_s is None: #devo ancora passare dal personalizza
                elementi_request = ''
                no_display = _("Can not display the requested content")
            else:
                elementi_request = "<br>".join(['%s:: %s' % (key, value) for (key, value) in request.REQUEST.items()])
                no_display = _("The requested content is empty")

        context['dataframe'] = no_display

        send_mail('Errore Lod4Stat (' +
                  str(request.user) + ') ' +
                  ''.join(ALLOWED_HOSTS) +
                  '/query_editor_view/?table=' +
                  table_name, unicode(no_display) + elementi_request, DEFAULT_FROM_EMAIL, ADMINISTRATOR_EMAIL,
                  fail_silently=False)

        return render_to_response("l4s/query_editor_view.html", context)

    store = store_data_frame(df)
    html = data_frame_to_html(df, visible, pivot)

    url = '/query_editor_view/?table=%s' % table_name

    context['dataframe'] = html
    context['store'] = store
    context['sql'] = sql
    context['url'] = url
    #context['table_external_metadata'] = table_external_metadata
    #context['column_external_metadata'] = column_external_metadata
    context['column_value_external_metadata'] = column_value_external_metadata

    #print datetime.now().strftime("%H:%M:%S.%f")

    """
    print 'obs_values ', context['obs_values']
    print 'selected_obs_values ', context['selected_obs_values']
    print 'aggregations ', context['aggregations']
    #print 'agg_values ', context['agg_values']
    #print 'aggregation_ids ', context['aggregation_ids']
    print 'topic ', context['topic']
    print 'topic_id ', context['topic_id']
    print 'hidden_fields ', context['hidden_fields']
    print 'columns ', context['columns']
    print 'rows ', context['rows']
    #print 'filters ', context['filters']
    #print 'agg_filters ', context['agg_filters']
    print 'sel_aggregate ', context['sel_aggregate']
    print 'debug ', context['debug']
    print 'include_code ', context['include_code']
    print 'range ', context['range']
    print 'ref_periods ', context['ref_periods']
    print 'legend ', context['legend']
    print 'dl_art ', context['dl_art']

    print 'description ', context['description']
    print 'sel_tab ', context['sel_tab']
    print 'agg_col ', context['agg_col']

    print 'column_description ', context['column_description']

    #print 'dataframe ', context['dataframe']
    #print 'store ', context['store']
    print 'sql ', context['sql']
    print 'url ', context['url']

    print "aggregations ", aggregations
    print "agg_values ", agg_values
    print "filters ", filters
    print "values ", values


    print "sel_aggregation ", aggregation
    print "agg_filters ", agg_filters


    print "not_sel_aggregations ", not_sel_aggregations
    print "not_agg_selection_value ", not_agg_selection_value
    """

    return render_to_response("l4s/query_editor_view.html", context)


@login_required()
def query_editor_save_done(request):
    """
    Shown after that the user save a query with editor.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    form = CreateQueryEditorForm(request.POST)
    if form.is_valid():
        pk = request.REQUEST.get('pk')
        if pk == "-1":
            form.save()
        else:
            pk_int = ast.literal_eval(pk)
            query = Query.objects.get(pk=pk_int)
            form = CreateQueryEditorForm(request.POST, instance=query)
            form.save()

        link = '/explorer?public=false'
        here = unicode(_("here"))
        body = unicode(_("The query has been saved")).replace('\'', '\\\'')
        body += '.<br>'
        body += unicode(_("In order to see the saved queries click"))
        body += ' <a href="%s">%s</a>' % (link, here)
        http_response = '<script type="text/javascript">'
        http_response += "$('#popup').modal('hide');"
        http_response += "bootbox.alert('%s');" % body
        http_response += '</script>'

        return HttpResponse(http_response)

    context['form'] = form
    return render_to_response("l4s/query_editor_save.html", context)


@login_required()
def query_editor_save_check(request):
    """
    Check that if exists a query
    with the same name for the same user.
    It return the public key of the query found or -1 if it is not found.

    :param request: Django request.
    :return: pk in HttpResponse.
    """

    title = request.REQUEST.get('title')
    author = request.REQUEST.get('created_by')
    try:
        query = Query.objects.get(title=title, created_by=author)
        pk = query.pk
    except ObjectDoesNotExist:
        pk = -1

    return HttpResponse(pk)


@login_required()
def query_editor_save(request):
    """
    Query editor save.

    :param request: Django request.
    :return: The request response.
    """

    context = RequestContext(request)

    sql = request.REQUEST.get('sql', '')
    title = request.REQUEST.get('title')
    description = request.REQUEST.get('description')
    table_name = request.REQUEST.get('table')
    columns = request.REQUEST.get('columns')
    rows = request.REQUEST.get('rows')
    obs_values = request.REQUEST.get('obs_values')
    aggregations = request.REQUEST.get('aggregations')
    filters = request.REQUEST.get('filters')
    agg_filters = request.REQUEST.get('agg_filters')
    include_code = request.REQUEST.get('include_code')
    range = request.REQUEST.get('range')
    not_sel_aggregations = request.REQUEST.get('not_sel_aggregations')
    not_agg_selection_value = request.REQUEST.get('not_agg_selection_value')

    #print "agg_filters '",agg_filters,"'"
    #print "not_agg_selection_value '",not_agg_selection_value,"'"


    form = CreateQueryEditorForm(
        initial={'sql': sql,
                 'title': title,
                 'description': description,
                 'created_by': request.user.email,
                 'query_editor': True,
                 'table': table_name,
                 'columns': columns,
                 'rows': rows,
                 'obs_values': obs_values,
                 'aggregations': aggregations,
                 'filters': filters,
                 'include_code': include_code,
                 'agg_filters': agg_filters,
                 'range':range,
                 'not_sel_aggregations':not_sel_aggregations,
                 'not_agg_selection_value':not_agg_selection_value})
    context['form'] = form
    return render_to_response("l4s/query_editor_save.html", context)


def query_editor(request):
    """
    Query editor.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    topic = request.GET.get('topic')
    search = request.GET.get('search', '')

    topic_dict = dict()
    topic_mapping = build_topics_decoder_dict()
    table_description = dict()
    tables = all_visible_tables(request)
    #print tables

    # Filter tables matching descriptions and table_name.
    if search:
        table_description = get_table_by_name_or_desc(search, tables, 'value')
        #print get_color()
        #print "table_description ", table_description
        tables = table_description.keys()

    if topic is not None and topic != "":
        topic_id = ast.literal_eval(topic)
    else:
        # Topic 0 means that all the topics will be displayed.
        topic_id = 0

    #print "topic_id", topic_id

    if topic_id == 0:
        context['topics_counter'] = build_topics_counter_dict(tables)
        #print "tables 1", tables
        tables = order_tables_by_topic_and_descriptions(tables)
        #print "tables 2", tables
        topic_dict = build_topics_dict(tables)
    else:
        tables = filter_tables_by_topic(topic_id, tables, None)
        #print "tables1 ", tables
        tables = exclude_invisible_tables(tables)
        #print "tables2 ", tables
        tables = order_tables_by_descriptions(tables)
        if not search:
            table_description = build_description_table_dict(tables)

    keywords = build_topic_keywords()
    icons = build_topic_icons()

    queries = Query.objects.filter(is_public='true')
    if search:
        queries = queries & (Query.objects.filter(title__icontains=search) |
                             Query.objects.filter(
                                 description__icontains=search))
    queries_to_topics = build_queries_to_topics_mapping(queries, topic_id)
    queries = order_queries_by_topics(queries_to_topics.items())

    context['topic'] = topic_id
    context['topics'] = topic_mapping
    context['table_list'] = tables
    context['search'] = search
    context['table_description'] = table_description
    context['topic_dict'] = topic_dict
    context['selected_topic'] = topic_id
    context['keywords'] = keywords
    context['icons'] = icons
    context['queries_to_topics'] = queries_to_topics
    context['queries'] = queries

    #print "topics ", topic_mapping
    #print "topics_counter ", context['topics_counter']

    """
    print get_color()
    print "topic_id" , topic_id
    print "topic_mapping", topic_mapping
    print "tables", tables
    print "search", search
    print "table_description", table_description
    print "topic_dict", topic_dict
    print "topic_id", topic_id
    print "keywords", keywords
    print "icons", icons
    print "queries_to_topics", queries_to_topics
    print "queries", queries
    """

    return render_to_response("l4s/query_editor.html", context)


def show_credits(request):
    """
    Show credits.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    return render_to_response("l4s/credits.html", context)


@user_passes_test(lambda u: u.is_staff)
def manual_request_list(request):
    """
    View of the manual requests.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    objects = ManualRequest.objects.filter(dispatched=False)
    context['object_list'] = objects
    return render_to_response("l4s/manual_request_list.html", context)


@user_passes_test(lambda u: u.is_staff)
def manual_request_history(request):
    """
    View of the manual requests.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    objects = ManualRequest.objects.filter(dispatched=True).order_by(
        'dispatch_date')
    context['object_list'] = objects
    return render_to_response("l4s/manual_request_history.html", context)


@user_passes_test(lambda u: u.is_staff)
def manual_request_view(request):
    """
    View of the manual request specified with id.

    :param request: Django request.
    :return: The Django request response.
    """
    if request.method == 'POST':
        manual_request_id = request.POST.get('id')
        if not manual_request is None and manual_request_id != "":
            item = ManualRequest.objects.get(id=manual_request_id)
            item.dispatched = True
            item.dispatch_date = datetime.now()
            dispatcher = request.POST.get('dispatcher', '')
            item.dispatcher = dispatcher
            dispatch_note = request.POST.get('dispatch_note', '')
            item.dispatch_note = dispatch_note
            item.save()
        return HttpResponseRedirect('/manual_request_list')

    context = RequestContext(request)
    manual_request_id = request.GET.get('id', '')
    item = ManualRequest.objects.get(id=manual_request_id)
    context['manual_request'] = item
    form = ManualRequestDispatchForm(
        initial={'dispatcher': request.user, 'id': manual_request_id})
    return render_to_response("l4s/manual_request_view.html",
                              {'form': form}, context)


@login_required
def manual_request_accepted(request):
    """
    Notify to the user that the manual request is accepted-.

    :param request: Django request.
    :return: The Django request response.
    """
    query_id = request.GET.get('id')
    context = RequestContext(request)
    context['id'] = query_id
    return render_to_response("l4s/manual_request_accepted.html", context)


@login_required()
def manual_request_save(request):
    """
    Save manual request.

    :param request: Django request.
    :return: The request response.
    """
    context = RequestContext(request)
    form = ManualRequestForm(request.POST)
    if form.is_valid():
        instance = form.save()
        url = '/manual_request_accepted/?id=' + str(instance.pk)
        email_new_manual_request(instance)
        email_manual_request_notification(instance, request.user.email)
        return redirect(url)

    return render_to_response('l4s/manual_request.html',
                              {'form': form},
                              context_instance=context)


@login_required
def manual_request(request):
    """
    Ask for a manual request.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)

    subject = request.REQUEST.get('title')
    url = request.REQUEST.get('url')
    topic = request.REQUEST.get('topic')
    if topic is None:
        topic_id = 1
    else:
        topic_id = ast.literal_eval(topic)

    context['title'] = subject
    context['districts'] = list_districts()
    context['valley_communities'] = list_valley_communities()
    context['tourism_sectors'] = list_tourism_sectors()
    context['health_districts'] = list_health_districts
    form = ManualRequestForm(initial={'inquirer': request.user,
                                      'url': url,
                                      'subject': subject,
                                      'topic': topic_id,
                                      'territorial_level': " "})

    return render_to_response('l4s/manual_request.html',
                              {'form': form},
                              context_instance=context)


@login_required
def user_profile(request):
    """
    Show user profile.

    :param request: Django request.
    :return: The Django request response.
    """
    return render_to_response("l4s/user_profile.html",
                              RequestContext(request))


@login_required
def user_profile_change(request):
    """
    Change the user profile.

    :param request: Django request.
    :return: The Django request response.
    """
    if request.method == 'POST':
        user = request.user
        form = UserChangeForm(request.POST, instance=user)
        if form.is_valid():
            form.save()
            return render_to_response("l4s/user_profile.html",
                                      RequestContext(request))
        else:
            return render_to_response('l4s/user_profile_change.html',
                                      {'form': form},
                                      context_instance=RequestContext(request))

    form = UserChangeForm(instance=request.user)
    return render_to_response('l4s/user_profile_change.html',
                              {'form': form},
                              context_instance=RequestContext(request))


def success(request):
    """
    Registration success page.

    :param request: Django request.
    :return: The Django request response.
    """
    return render_to_response("account/success.html",
                              RequestContext(request))


def about(request):
    """
    About page.

    :param request: Django request.
    :return: The Django request response.
    """
    return render_to_response("l4s/about.html",
                              RequestContext(request))


def get_list_of_value(request):
    #test

    lista_valori =  json.loads( request.POST.get('lista_valori') )
    id_agg = request.POST.get('id_agg')

    query = "select a.table_name, a.column_name, b.table_name, b.column_name " \
            "from  web_metadata a join web_metadata b on (b.key = 'http://purl.org/linked-data/cube#concept' and b.value = a.value) " \
            "where a.id = " + id_agg

    rows= execute_query_on_django_db(query)

    for row in rows:
        source_table = row[0];
        source_column = row[1];
        destination_table = row[2];
        destination_column = row[3];

    query = "select a." + source_column + " " \
            "from " + source_table + " a join " + destination_table + " b on (a." + source_column + "=b." + source_column + ") " \
            "where b." + destination_column + " in (" + ', '.join(lista_valori) + ")"

    rows = execute_query_on_main_db(query)

    lista_elementi = []

    for row in rows:
        lista_elementi.append(str(row[0]))

    return HttpResponse(",".join(lista_elementi))


def FAQ(request):
    """
    About page.

    :param request: Django request.
    :return: The Django request response.
    """
    return render_to_response("l4s/FAQ.html",
                              RequestContext(request))


def manual_view(request):

    #print request.user.is_authenticated()
    #print request.user.is_superuser
    #print request.user.is_staff

    path = 'l4s/static/templates/l4s/'

    #print "user " , request.user

    if request.user.is_authenticated() == False:
        nome_file = 'Manuale_Utente_Anonimo_LOD4STAT.pdf'
    else:
        if request.user.is_superuser == True:
            nome_file = 'Manuale_Amministratore_LOD4STAT.pdf'
        else:
            if request.user.is_staff == True:
                nome_file = 'Manuale_Utente_Interno_LOD4STAT.pdf'
            else:
                nome_file = 'Manuale_Utente_Registrato_LOD4STAT.pdf'

    with open(path + nome_file, 'r') as pdf:
        response = HttpResponse(pdf.read(), mimetype='application/pdf')
        response['Content-Disposition'] = 'inline;filename=' + nome_file
        return response
    pdf.closed

def sync(request):

    table = request.POST.getlist('table[]')

    f = open("blah.txt", "w")
    result = subprocess.call(shlex.split('./synct.sh ' + ",".join(table)), stdout=f)

    send_mail("Result", str(result), DEFAULT_FROM_EMAIL, ['m.voltolini@trentinosistemi.com'], fail_silently=False)

    query = "SELECT success FROM %s \n" % SYNCHRONIZATION
    query += "ORDER BY start_time DESC \n"
    query += "LIMIT 1"

    #print query

    rows = execute_query_on_main_db(query)

    for row in rows:
        if (row[0] == True):
            stringa = "Sincronizzazione avvenuta con successo !"
        else:
            stringa = "Sincronizzazione NON avvenuta con successo !"

    return HttpResponse(stringa)