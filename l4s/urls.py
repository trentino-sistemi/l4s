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

from django.urls import include, re_path
from django.contrib import admin, auth
from web import views
from web.views import QueryView, CreateQueryView
from django.conf import settings
from django.shortcuts import render
from django.core.mail import send_mail
import json
from web.utils import get_client_ip
from django.http import HttpResponse
from django.views.static import serve
from django.conf.urls.static import static

urlpatterns = [
                       re_path(r'^robots.txt$', lambda r: HttpResponse("User-agent: *\nAllow: /about\nDisallow: /", content_type="text/plain")),
                       # Url for admin interface.
                       re_path(r'^admin/',
                           admin.site.urls),
                       # Open data queries.
                       re_path(r'^open_data/',
                           views.open_data,
                           name='open_data'),
                      # Url to download usage report in excel 97 format.
                       re_path(r'^usage_report/download_xls',
                           views.usage_report_download_xls,
                           name='usage_report_download_xls'),
                       #Url for usage report.
                       re_path(r'^usage_report/',
                           views.usage_report,
                           name='usage_report'),
                       #Url for query editor customize.
                       re_path(r'^query_editor_customize/$',
                           views.query_editor_customize,
                           name='query_editor_customize'),
                       #Url for query editor external metadata.
                       re_path(r'^query_editor_external_metadata/$',
                           views.query_editor_external_metadata,
                           name='query_editor_external_metadata'),
                       #Url for query editor save check.
                       re_path(r'^query_editor_save_check/$',
                           views.query_editor_save_check,
                           name='query_editor_save_check'),
                       #Url for query editor save done.
                       re_path(r'^query_editor_save_done/$',
                           views.query_editor_save_done,
                           name='query_editor_save_done'),
                       #Url for query editor save.
                       re_path(r'^query_editor_save/$',
                           views.query_editor_save,
                           name='query_editor_save'),
                       #Url for query view editor.
                       re_path(r'^query_editor_view/$',
                           views.query_editor_view,
                           name='query_editor_view'),
                       #Url for query editor.
                       re_path(r'^query_editor/',
                           views.query_editor,
                           name='query_editor'),
                       #Url for not implemented features.
                       re_path(r'^no_implemented/',
                           views.no_implemented,
                           name='no_implemented'),
                       # Url for about.
                       re_path(r'^about/',
                           views.about,
                           name='about'),
                       # Url to show table list of source database.
                       re_path(r'^source_table_list/$',
                           views.source_table_list,
                           name='source_table_list'),
                       # Url to show the table list.
                       re_path(r'^table_list/$',
                           views.table_list,
                           name='table_list'),
                       # Url to add the table-column metadata.
                       re_path(r'^table/metadata/add/$',
                           views.table_add_metadata,
                           name='table_add_metadata'),
                       # Url to edit the table-column metadata.
                       re_path(r'^table/metadata/edit/$',
                           views.table_edit_metadata,
                           name='table_edit_metadata'),
                       # Url to del the table-column metadata.
                       re_path(r'^table/metadata/delete/$',
                           views.table_delete_metadata,
                           name='table_delete_metadata'),
                       # Url to view the table-column metadata.
                       re_path(r'^table/metadata/$',
                           views.table_view_metadata,
                           name='table_view_metadata'),
                       # Page to view,ontologies.
                       re_path(r'^ontology/$',
                           views.ontology,
                           name='ontology'),
                       # Page to add,ontology.
                       re_path(r'^add_ontology/$',
                           views.add_ontology,
                           name='add_ontology'),
                       # Url to del an ontology file.
                       re_path(r'^ontology/delete/$',
                           views.delete_ontology,
                           name='delete_ontology'),
                       # Download query in csv format.
                       re_path(r'^download_csv',
                           views.query_download_csv,
                           name='query_download_csv'),
                       # Download query in xlsx Excel 2007 format.
                       re_path(r'^download_xlsx',
                           views.query_download_xlsx,
                           name='query_download_xlsx'),
                       # Download query in excel 97 format.
                       re_path(r'^download_xls',
                           views.query_download_xls,
                           name='query_download_xls'),
                       # Download query in json-stat format.
                       re_path(r'^download_json_stat',
                           views.query_download_json_stat,
                           name='query_download_json_stat'),
                       # Download query in sdmx format.
                       re_path(r'^download_sdmx',
                           views.query_download_sdmx,
                           name='query_download_sdmx'),
                       # Download query in rdf format.
                       re_path(r'^download_rdf',
                           views.query_download_rdf,
                           name='query_download_rdf'),
                       # Download query in turtle format.
                       re_path(r'^download_turtle',
                           views.query_download_turtle,
                           name='query_download_turtle'),
                       # Url to show table structure.
                       re_path(r'^table/$',
                           views.table,
                           name='table'),
                       # Url to view the manual requests.
                       re_path(r'^manual_request_list/$',
                           views.manual_request_list,
                           name='manual_request_list'),
                       # Url to view the manual requests history.
                       re_path(r'^manual_request_history/$',
                           views.manual_request_history,
                           name='manual_request_history'),
                       # Url to save the manual request specified..
                       re_path(r'^manual_request_save/$',
                           views.manual_request_save,
                           name='manual_request_save'),
                       # Url to view the manual request specified with id.
                       re_path(r'^manual_request_view/$',
                           views.manual_request_view,
                           name='manual_request_view'),
                       # Url to ask a manual request.
                       re_path(r'^manual_request/$',
                           views.manual_request,
                           name='manual_request'),
                       # Url that notify to the user that the manual request
                       # has been accepted.
                       re_path(r'^manual_request_accepted/$',
                           views.manual_request_accepted,
                           name='manual_request_accepted'),
                       # Url to empty table.
                       re_path(r'^empty_table/$',
                           views.empty_table,
                           name='empty_table'),
                       # Url to generate test tables.
                       re_path(r'^test_table/$',
                           views.test_table,
                           name='test_table'),
                       # Url for initial page.
                       re_path(r'^$',
                           views.index,
                           name='index'),
                       # Url to change the user profile of the logged user.
                       re_path(r'^user_profile/change',
                           views.user_profile_change,
                           name='user_profile_change'),
                       # Url to show the user profile of the logged user.
                       re_path(r'^user_profile',
                           views.user_profile,
                           name='user_profile'),
                       # Url to show legal notes.
                       re_path(r'^legal_notes',
                           views.legal_notes,
                           name='legal_notes'),
                       # Url to show privacy policy.
                       re_path(r'^privacy_policy',
                           views.privacy_policy,
                           name='privacy_policy'),
                       # Url to show credits.
                       re_path(r'^credits',
                           views.show_credits,
                           name='show_credits'),
                       # Welcome.
                       re_path(r'^accounts/success/$',
                           views.success,
                           name='success'),
                       # Remove the logout confirmation step.
                       re_path(r'^accounts/logout/$',
                           auth.views.LogoutView.as_view(),
                           {'next_page': '/'}),
                       re_path(r'^accounts/password/change/$',
                           views.redirect_after_password_change,
                           name='account_change_password'),  # Place before allauth urls to override
                       # Django registration urls.
                       re_path(r'^accounts/',
                           include('allauth.urls')),
                       # Create a new query.
                       re_path(r'^explorer/new/$', CreateQueryView.as_view(),
                           name='query_create'),
                       # In order to copy a public query in the personal one.
                       re_path(r'^explorer/copy/$',
                           views.query_copy,
                           name='query_copy'),
                       # Override django sql explorer with a custom view
                       # with custom form with custom validators.
                       re_path(r'^explorer/(?P<query_id>\d+)/$',
                           QueryView.as_view(),
                           name='query_detail'),
                       # Recent queries.
                       re_path(r'^explorer/recent_queries/$',
                           views.recent_queries,
                           name='recent_queries'),
                       # List the django explorer query using a custom view.
                       re_path(r'^explorer/$',
                           views.query_list,
                           name='query_list'),
                       # Override explorer delete.
                       re_path(r'(?P<pk>\d+)/delete$',
                           views.delete_query,
                           name='query_delete'),
                       # Django explorer urls.
                       re_path(r'^explorer/',
                           include('explorer.urls')),
                       re_path(r'^get_list_of_value/$',
                           views.get_list_of_value,
                           name='get_list_of_value'),
                       # Url for FAQ.
                       re_path(r'^FAQ/',
                           views.FAQ,
                           name='FAQ'),
                       # Url for PDF manual.
                       re_path(r'^manual/',
                           views.manual_view,
                           name='manual_view'),
                       re_path(r'^sync/',
                           views.sync,
                           name='sync'),
                       ] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.DEBUG is False:   #if DEBUG is True it will be served automatically
    urlpatterns += [
        re_path(r'^static/(?P<path>.*)$', serve,{'document_root': settings.STATIC_ROOT}),
        re_path(r'^media/(?P<path>.*)$', serve, {'document_root': settings.MEDIA_ROOT}),
    ]

def handler500(request):

    ip_address = get_client_ip(request)

    url = request.method + ' ' + request.get_full_path()

    if request.method == 'GET':
        elementi = {k: v if len(v) > 1 else v[0] for k, v in request.GET.lists()}
    elif request.method == 'POST':
        elementi = {k: v if len(v) > 1 else v[0] for k, v in request.POST.lists()}

    send_mail('Errore Lod4Stat (' + ip_address + ' ' + str(request.user) + ') ' + url, json.dumps(elementi), settings.DEFAULT_FROM_EMAIL, settings.ADMINISTRATOR_EMAIL, fail_silently=False)

    response = render(request, 'l4s/500.html', {}, context_instance={})
    response.status_code = 500
    return response

