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

from copy import deepcopy
from django.db import connections
from django.http import HttpResponseRedirect, HttpResponse
from django.shortcuts import render_to_response, \
    get_object_or_404, redirect
from django.template import RequestContext, Context
from django.utils.decorators import method_decorator
from django.utils.translation import ugettext_lazy as _
from django.views.generic.base import View
from django.views.generic.edit import CreateView
from l4s.settings import EXPLORER_PERMISSION_CHANGE, \
    EXPLORER_RECENT_QUERY_COUNT, \
    EXPLORER_CONNECTION_NAME, \
    EXPLORER_PERMISSION_VIEW
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
    filter_table_by_name_or_desc, \
    dataframe_to_html, \
    choose_default_axis, \
    filter_coder_table, \
    build_query, get_obs_value_columns,\
    change_query, \
    get_all_aggregations, \
    build_description_query, \
    get_all_field_values, \
    get_all_field_values_agg, \
    build_query_title, \
    build_query_summary, \
    build_query_desc, \
    load_dataframe, \
    store_dataframe
from web.statistical_secret import apply_stat_secret, \
    detect_special_columns, \
    apply_stat_secret_plain, \
    headers_and_data
from web.topics import build_topics_dict, \
    filter_tables_by_topic, \
    build_topics_counter_dict
from explorer.views import ExplorerContextMixin, \
    view_permission, \
    reverse_lazy
from explorer.models import Query
from explorer.utils import url_get_rows
from web.actions import generate_report_action_csv, \
    generate_report_action_xls, \
    generate_report_action_xlsx, \
    generate_report_action_sdmx,\
    generate_report_action_rdf, \
    generate_report_action_turtle, \
    generate_report_action_json_stat
from datetime import datetime
from django.contrib.auth.decorators import login_required, \
    user_passes_test
from django.utils import translation
import json


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
        query.sql = build_description_query(query.sql, st.cols, False)

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
                                                              debug)
            if warn_n is not None:
                warn = warn_n

        if df is not None:
            store = store_dataframe(request, df)
            html = dataframe_to_html(df, pivot)

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
    :param title:
    :param form:
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
        save = request.REQUEST.get('save', '')
        to_be_saved = False
        if save and save == 'true':
            to_be_saved = True

        pivot_column = request.REQUEST.get('pivot_column', '')
        pivot_cols = []
        for i in pivot_column.split(','):
            if i.isdigit():
                pivot_cols.append(int(i))

        aggregation = request.REQUEST.get('aggregate', '')

        aggregation_ids = []
        for i in aggregation.split(','):
            if i.isdigit():
                aggregation_ids.append(int(i))

        debug = False
        if request.user.is_staff:
            debug_s = request.REQUEST.get('debug', '')
            if debug_s and debug_s == 'true':
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

        save = request.REQUEST.get('save', '')
        to_be_saved = False
        if save and save == 'true':
            to_be_saved = True

        aggregation = request.REQUEST.get('aggregate', '')

        aggregation_ids = []
        for i in aggregation.split(','):
            if i.isdigit():
                aggregation_ids.append(int(i))

        debug = False
        if request.user.is_staff:
            debug_s = request.REQUEST.get('debug', '')
            if debug_s and debug_s == 'true':
                debug = True

        pivot_column = request.GET.get('pivot_column', '')
        pivot_cols = []
        message = None
        for i in pivot_column.split(','):
            if i.isdigit():
                pivot_cols.append(int(i))

        if not EXPLORER_PERMISSION_VIEW(request.user):
            return HttpResponseRedirect(
                reverse_lazy('query_detail', kwargs={'query_id': query_id})
            )

        query = get_object_or_404(Query, pk=query_id)
        url = '/explorer/%d/' % query.pk

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


@view_permission
def query_download_csv(request):
    """
    Download query in CSV format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    fn = generate_report_action_csv(df)
    title = request.REQUEST.get('title')
    return fn(title)


def query_download_xls(request):
    """
    Download query in Excel 97 format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    fn = generate_report_action_xls(df)
    title = request.REQUEST.get('title')
    description = request.REQUEST.get('description')
    return fn(title, description)


def query_download_xlsx(request):
    """
    Download query in Excel xlsx 2007 format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    fn = generate_report_action_xlsx(df)
    title = request.REQUEST.get('title')
    description = request.REQUEST.get('description')
    return fn(title, description)


def query_download_json_stat(request):
    """
    Download query in Json stat format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    title = request.REQUEST.get('title')

    fn = generate_report_action_json_stat(df)
    return fn(title)


def query_download_sdmx(request):
    """
    Download query in Sdmx xlm message format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    title = request.REQUEST.get('title')
    fn = generate_report_action_sdmx(df)
    sql = request.REQUEST.get('sql')

    return fn(title, sql)


def query_download_rdf(request):
    """
    Download query in Rdf data cube format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    title = request.REQUEST.get('title')
    sql = request.REQUEST.get('sql')
    description = request.REQUEST.get('description')

    fn = generate_report_action_rdf(df)

    return fn(title, description, sql)


def query_download_turtle(request):
    """
    Download query in Rdf data cube format.

    :param request: Django request.
    :return: The request response.
    """
    df = load_dataframe(request)
    title = request.REQUEST.get('title')
    sql = request.REQUEST.get('sql')
    description = request.REQUEST.get('description')

    fn = generate_report_action_turtle(df)

    return fn(title, description, sql)


@user_passes_test(lambda u: u.is_staff)
def empty_table(request):
    """
    Delete the table passed by url.

    :param request: Django request.
    :return: The request response.
    """
    table_name = request.GET.get('table', '')
    delete_all(table_name)
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

    table_name = request.GET.get('table', '')
    column_name = request.GET.get('column', '')
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
    metadata_id = request.GET.get('id', '')
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
    if request.method == 'POST':
        metadata_id = request.POST.get('id', '')

        metadata = Metadata.objects.get(id=metadata_id)
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

    metadata_id = request.GET.get('id', '')
    metadata = Metadata.objects.get(id=metadata_id)
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
    :return: The Django request response.
    """
    table_name = request.GET.get('table', '')
    column_name = request.GET.get('column', '')
    if column_name:
        metadata_list = Metadata.objects.filter(table_name=table_name,
                                                column_name=column_name)
    else:
        metadata_list = Metadata.objects.filter(table_name=table_name)
    context = Context({'metadata_list': metadata_list})
    if table_name:
        context['table_name'] = table_name
    if column_name:
        context['column_name'] = column_name
    context['request'] = request
    return render_to_response('l4s/metadata.html', context)


@user_passes_test(lambda u: u.is_staff)
def delete_ontology(request):
    """
    Delete ontology.

    :param request: Django request.
    :return: The Django request response.
    """
    ontology_id = request.GET.get('id', '')
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

    :param request:
    :return: The Django request response.
    """
    return render_to_response('l4s/no_implemented.html')


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
    context['topics'] = build_topics_dict()
    return render_to_response("l4s/source_table_list.html", context)


@login_required
def table_list(request):
    """
    List the tables in the main database.

    :param request: Django request.
    :return: The Django request response.
    """

    topic = request.GET.get('topic', '')
    # Topic 0 means that all the topics will be displayed.
    topic_id = 0
    if topic.isdigit():
        topic_id = int(topic)

    connection = connections[EXPLORER_CONNECTION_NAME]
    tables = connection.introspection.table_names()
    table_description_dict = build_description_table_dict(tables)

    if topic_id is not 0:
        tables = filter_tables_by_topic(topic_id, tables)

    search = request.GET.get('search', '')
    # Filter tables matching descriptions and table_name.
    if search:
        tables = filter_table_by_name_or_desc(search, tables,
                                              table_description_dict)

    context = Context({'table_list': tables})
    context['request'] = request
    context['table_description_dict'] = table_description_dict
    context['topics'] = build_topics_dict()
    context['selected_topic'] = topic_id
    return render_to_response("l4s/table_list.html", context)


@login_required
def table(request):
    """
    Request to view a table structure.

    :param request: Django request.
    :return: The Django request response.
    """
    table_name = request.GET.get('name', '')
    table_schema = get_table_schema(table_name)
    column_description = build_description_column_dict(table_name,
                                                       table_schema)
    context = Context({'table_schema': table_schema})
    context['table_name'] = table_name
    context['request'] = request
    context['column_description'] = column_description
    return render_to_response("l4s/table.html", context)


def query_list(request):
    """
    List the queries using this custom view.
    This is used for logged users only.

    :param request: Django request.
    :return: The Django request response.
    """

    search = request.GET.get('search', '')
    objects = (Query.objects.filter(created_by=request.user) |
               Query.objects.filter(is_public='true'))
    if search:
        objects = objects & (Query.objects.filter(title__icontains=search) |
                             Query.objects.filter(
                                 description__icontains=search))
    criteria = "None"
    order_by = request.GET.get('order_by', '')
    if order_by:
        criteria = order_by
        objects = objects.order_by(criteria)
    else:
        objects = objects.order_by('-created_at')

    context = Context({'object_list': objects})
    context['request'] = request
    context['can_change'] = EXPLORER_PERMISSION_CHANGE(request.user)
    context['order_by'] = criteria
    return render_to_response("explorer/query_list.html", context)


@user_passes_test(lambda u: u.is_staff)
def recent_queries(request):
    """
    List the recent queries.
    This is used for internal users only.

    :param request: Django request.
    :return: The Django request response.
    """
    search = request.GET.get('search', '')
    objects = Query.objects
    if search:
        objects = objects & (Query.objects.filter(title__icontains=search) |
                             Query.objects.filter(
                                 description__icontains=search))

    recent = objects.order_by('-last_run_date')
    recent = recent[:EXPLORER_RECENT_QUERY_COUNT]
    context = Context({'object_list': objects})
    context['request'] = request
    context['can_change'] = EXPLORER_PERMISSION_CHANGE(request.user)
    context['recent_queries'] = recent
    return render_to_response("explorer/recent_queries.html", context)


@login_required
def query_copy(request):
    """
    Copy public query in the query of logged  user.

    :param request: Django request.
    :return: The Django request response.
    """
    query_id = request.GET.get('id', '')
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
    search = request.GET.get('search', '')
    objects = Query.objects.filter(is_public=True)
    if search:
        found = Query.objects.filter(title__icontains=search)
        found = found | Query.objects.filter(description__icontains=search)
        objects = objects & found

    context = RequestContext(request)
    context['object_list'] = objects
    return render_to_response("l4s/index.html", context)


def legal_notes(request):
    """
    Show legal notes.

    :param request: Django request.
    :return: The Django request response.
    """
    context = RequestContext(request)
    return render_to_response("l4s/legal_notes.html", context)


def query_editor_customize(request):
    """
    Customize the default query in editor.

    :param request:
    :return:
    """

    context = RequestContext(request)
    debug = False
    if request.user.is_staff:
        debug_s = request.REQUEST.get('debug', '')
        if debug_s and debug_s == 'true':
            debug = True

    table_name = request.REQUEST.get('table', '')
    columns = request.REQUEST.get('columns', '')
    cols = []
    if columns != "":
        for i in columns.split(','):
            cols.append(i)

    rows_s = request.REQUEST.get('rows', '')
    rows = []
    if rows_s != "":
        for i in rows_s.split(','):
            rows.append(i)

    sel_aggregation = request.REQUEST.get('aggregate', '')

    sel_aggregation_ids = []
    for i in sel_aggregation.split(','):
        if i.isdigit():
            sel_aggregation_ids.append(i)

    table_schema = get_table_schema(table_name)
    column_description = request.REQUEST.get('column_description')

    fields = [field.name for field in table_schema]
    obs_values = get_obs_value_columns(table_name, table_schema)

    context['fields'] = fields
    context['obs_values'] = obs_values
    context['table_name'] = table_name
    context['columns'] = cols
    context['rows'] = rows

    values = request.REQUEST.get('values')
    agg_values = request.REQUEST.get('agg_values')
    aggregations = request.REQUEST.get('aggregations')

    filters = request.REQUEST.get('filters')

    agg_filters = request.REQUEST.get('agg_filters')

    context['aggregations'] = json.loads(aggregations)
    context['sel_aggregation'] = sel_aggregation_ids
    context['column_description'] =  json.loads(column_description)
    context['debug'] = debug
    context['values'] = json.loads(values)
    context['agg_values'] = json.loads(agg_values)
    context['filters'] = json.loads(filters)
    context['agg_filters'] = json.loads(agg_filters)

    return render_to_response("l4s/query_editor_customize.html", context)


def query_editor_view(request):
    """
    Query editor view.

    :param request:
    :return:
    """
    table_name = request.REQUEST.get('table', '')
    context = RequestContext(request)
    context['table_name'] = table_name
    table_description_dict = build_description_table_dict([table_name])
    context['table_description'] = table_description_dict[table_name]

    filters_s = request.REQUEST.get('filters')
    agg_filters_s = request.REQUEST.get('agg_filters')

    columns = request.REQUEST.get('columns', '')

    cols = []
    if columns != "":
        for i in columns.split(','):
            cols.append(i)

    rows_s = request.REQUEST.get('rows', '')
    rows = []
    if rows_s != "":
        for i in rows_s.split(','):
            rows.append(i)

    debug = False
    if request.user.is_staff:
        debug_s = request.REQUEST.get('debug', '')
        if debug_s and debug_s == 'true':
            debug = True

    aggregation = request.REQUEST.get('aggregate', '')

    aggregation_ids = []
    for i in aggregation.split(','):
        if i.isdigit():
            aggregation_ids.append(i)

    values = dict()
    table_description = get_table_schema(table_name)

    obs_values = get_obs_value_columns(table_name, table_description)
    for f, field in enumerate(table_description):
        column_name = field.name
        if not column_name in obs_values:
            vals = get_all_field_values(table_name, column_name)
            values[column_name] = vals

    table_schema = get_table_schema(table_name)
    aggregations = get_all_aggregations(table_name, table_schema)

    agg_values = dict()
    for agg in aggregations:
        for ag in aggregations[agg]:
            agg_new = dict()
            agg_new[ag] = agg
            vals = get_all_field_values_agg(ag)
            agg_values[ag] = vals

    if len(rows) + len(cols) == 0:
        cols, rows = choose_default_axis(table_name)
        if cols == -1:
            add = unicode(_("Please add the metadata 'obsValue'"))
            choose = unicode(_("on one of the columns of the table"))
            context['error'] = "%s %s '%s'" % (add, choose, table_name)

            return render_to_response("l4s/query_editor_view.html", context)

        filters = values
        agg_filters = agg_values
    else:
        filters = json.loads(filters_s)
        agg_filters = json.loads(agg_filters_s)

    sql, pivot = build_query(table_name, cols, rows, aggregation_ids,
                             filters, values)

    column_description = build_description_column_dict(table_name,
                                                       table_schema)

    query = Query(title=table_name, sql=sql)

    df, data, warn, err = headers_and_data(query,
                                           aggregation_ids,
                                           agg_filters,
                                           pivot,
                                           debug,
                                           True)

    title = build_query_title(df)

    agg_col, sel_tab = build_query_summary(column_description,
                                           values,
                                           filters,
                                           aggregation_ids,
                                           agg_values,
                                           agg_filters,
                                           df.columns.names,
                                           df.index.names)

    description = build_query_desc(agg_col, sel_tab)
    store = store_dataframe(request, df)
    html = dataframe_to_html(df, pivot)

    url = '/query_editor_view/?table=%s' % table_name

    context['dataframe'] = html
    context['values'] = values
    context['aggregations'] = aggregations
    context['agg_values'] = agg_values
    context['aggregation_ids'] = aggregation_ids
    context['column_description'] = column_description

    context['store'] = store
    context['sql'] = sql
    context['description'] = description
    context['title'] = title
    context['url'] = url
    context['columns'] = ",".join(cols)
    context['rows'] = ",".join(rows)
    context['filters'] = filters
    context['agg_filters'] = agg_filters
    context['sel_aggregate'] = aggregation
    context['debug'] = debug
    context['sel_tab'] = sel_tab
    context['agg_col'] = agg_col

    return render_to_response("l4s/query_editor_view.html", context)


def query_editor_save_done(request):
    context = RequestContext(request)
    form = CreateQueryEditorForm(request.POST)
    if form.is_valid():
        form.save()
        return HttpResponse('<script type="text/javascript">'
                            'window.close();'
                            'window.parent.location.href = "/";'
                            '</script>')
    context['form'] = form
    return render_to_response("l4s/query_editor_save.html", context)


def query_editor_save(request):
    """
    Query editor save.

    :param request:
    :return:
    """

    context = RequestContext(request)

    sql = request.REQUEST.get('sql', '')
    title = request.REQUEST.get('title')
    description = request.REQUEST.get('description')
    table_name = request.REQUEST.get('table')
    columns = request.REQUEST.get('columns')
    rows = request.REQUEST.get('rows')
    aggregations = request.REQUEST.get('aggregations')
    filters = request.REQUEST.get('filters')
    agg_filters = request.REQUEST.get('agg_filters')

    form = CreateQueryEditorForm(
        initial={'sql': sql,
                 'title': title,
                 'description': description,
                 'created_by': request.user.email,
                 'query_editor': True,
                 'table': table_name,
                 'columns': columns,
                 'rows': rows,
                 'aggregations': aggregations,
                 'filters': filters,
                 'agg_filters': agg_filters})
    context['form'] = form
    return render_to_response("l4s/query_editor_save.html", context)


def query_editor(request):
    """
    Query editor.

    :param request:
    :return:
    """
    context = RequestContext(request)
    topic = request.GET.get('topic', '')
    search = request.GET.get('search', '')
    # Topic 0 means that all the topics will be displayed.
    topic_id = 0
    if topic.isdigit():
        topic_id = int(topic)

    tables = []
    if topic_id is not 0 or search:
        connection = connections[EXPLORER_CONNECTION_NAME]
        tables = connection.introspection.table_names()
    if topic_id is not 0:
        tables = filter_tables_by_topic(topic_id, tables)
    else:
        context['topics_counter'] = build_topics_counter_dict()

    table_description = build_description_table_dict(tables)
    # Filter tables matching descriptions and table_name.
    if search:
        tables = filter_table_by_name_or_desc(search, tables,
                                              table_description)
    tables = filter_coder_table(tables)
    context['topics'] = build_topics_dict()
    context['table_list'] = tables
    context['topic'] = topic_id
    context['search'] = search
    context['table_description'] = table_description

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
        manual_request_id = request.POST.get('id', '')
        dispatcher = request.POST.get('dispatcher', '')
        dispatch_note = request.POST.get('dispatch_note', '')
        item = ManualRequest.objects.get(id=manual_request_id)
        item.dispatched = True
        item.dispatch_date = datetime.now()
        item.dispatcher = dispatcher
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
    query_id = request.GET.get('id', '')
    context = RequestContext(request)
    context['id'] = query_id
    return render_to_response("l4s/manual_request_accepted.html", context)


def manual_request_save(request):
    """
    Save manual request.

    :param request:
    :return:
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
    context['title'] = subject
    context['districts'] = list_districts()
    context['valley_communities'] = list_valley_communities()
    context['tourism_sectors'] = list_tourism_sectors()
    context['health_districts'] = list_health_districts
    form = ManualRequestForm(initial={'inquirer': request.user,
                                      'url': url,
                                      'subject': subject,
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
