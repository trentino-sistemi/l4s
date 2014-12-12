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

from django.conf.urls import patterns, include, url
from django.contrib import admin
from web.views import QueryView, CreateQueryView

admin.autodiscover()

urlpatterns = patterns('',
                       # Url for admin interface.
                       url(r'^admin/',
                           include(admin.site.urls)),
                       # Open data queries.
                       url(r'^open_data/',
                           'web.views.open_data',
                           name='open_data'),
                       #Url for query editor customize.
                       url(r'^query_editor_customize/$',
                           'web.views.query_editor_customize',
                           name='query_editor_customize'),
                       #Url for query editor save check.
                       url(r'^query_editor_save_check/$',
                           'web.views.query_editor_save_check',
                           name='query_editor_save_check'),
                       #Url for query editor save done.
                       url(r'^query_editor_save_done/$',
                           'web.views.query_editor_save_done',
                           name='query_editor_save_done'),
                       #Url for query editor save.
                       url(r'^query_editor_save/$',
                           'web.views.query_editor_save',
                           name='query_editor_save'),
                       #Url for query view editor.
                       url(r'^query_editor_view/$',
                           'web.views.query_editor_view',
                           name='query_editor_view'),
                       #Url for query editor.
                       url(r'^query_editor/',
                           'web.views.query_editor',
                           name='query_editor'),
                       #Url for not implemented features.
                       url(r'^no_implemented/',
                           'web.views.no_implemented',
                           name='no_implemented'),
                       # Url for about.
                       url(r'^about/',
                           'web.views.about',
                           name='about'),
                       # Url to show table list of source database.
                       url(r'^source_table_list/$',
                           'web.views.source_table_list',
                           name='source_table_list'),
                       # Url to show the table list.
                       url(r'^table_list/$',
                           'web.views.table_list',
                           name='table_list'),
                       # Url to add the table-column metadata.
                       url(r'^table/metadata/add/$',
                           'web.views.table_add_metadata',
                           name='table_add_metadata'),
                       # Url to edit the table-column metadata.
                       url(r'^table/metadata/edit/$',
                           'web.views.table_edit_metadata',
                           name='table_edit_metadata'),
                       # Url to del the table-column metadata.
                       url(r'^table/metadata/delete/$',
                           'web.views.table_delete_metadata',
                           name='table_delete_metadata'),
                       # Url to view the table-column metadata.
                       url(r'^table/metadata/$',
                           'web.views.table_view_metadata',
                           name='table_view_metadata'),
                       # Page to view,ontologies.
                       url(r'^ontology/$',
                           'web.views.ontology',
                           name='ontology'),
                       # Page to add,ontology.
                       url(r'^add_ontology/$',
                           'web.views.add_ontology',
                           name='add_ontology'),
                       # Url to del an ontology file.
                       url(r'^ontology/delete/$',
                           'web.views.delete_ontology',
                           name='delete_ontology'),
                       # Download query in csv format.
                       url(r'^download_csv',
                           'web.views.query_download_csv',
                           name='query_download_csv'),
                       # Download query in xlsx Excel 2007 format.
                       url(r'^download_xlsx',
                           'web.views.query_download_xlsx',
                           name='query_download_xlsx'),
                       # Download query in excel 97 format.
                       url(r'^download_xls',
                           'web.views.query_download_xls',
                           name='query_download_xls'),
                       # Download query in json-stat format.
                       url(r'^download_json_stat',
                           'web.views.query_download_json_stat',
                           name='query_download_json_stat'),
                       # Download query in sdmx format.
                       url(r'^download_sdmx',
                           'web.views.query_download_sdmx',
                           name='query_download_sdmx'),
                       # Download query in rdf format.
                       url(r'^download_rdf',
                           'web.views.query_download_rdf',
                           name='query_download_rdf'),
                       # Download query in turtle format.
                       url(r'^download_turtle',
                           'web.views.query_download_turtle',
                           name='query_download_turtle'),
                       # Url to show table structure.
                       url(r'^table/$',
                           'web.views.table',
                           name='table'),
                       # Url to view the manual requests.
                       url(r'^manual_request_list/$',
                           'web.views.manual_request_list',
                           name='manual_request_list'),
                       # Url to view the manual requests history.
                       url(r'^manual_request_history/$',
                           'web.views.manual_request_history',
                           name='manual_request_history'),
                       # Url to save the manual request specified..
                       url(r'^manual_request_save/$',
                           'web.views.manual_request_save',
                           name='manual_request_save'),
                       # Url to view the manual request specified with id.
                       url(r'^manual_request_view/$',
                           'web.views.manual_request_view',
                           name='manual_request_view'),
                       # Url to ask a manual request.
                       url(r'^manual_request/$',
                           'web.views.manual_request',
                           name='manual_request'),
                       # Url that notify to the user that the manual request
                       # has been accepted.
                       url(r'^manual_request_accepted/$',
                           'web.views.manual_request_accepted',
                           name='manual_request_accepted'),
                       # Url to empty table.
                       url(r'^empty_table/$',
                           'web.views.empty_table',
                           name='empty_table'),
                       # Url to generate test tables.
                       url(r'^test_table/$',
                           'web.views.test_table',
                           name='test_table'),
                       # Url for initial page.
                       url(r'^$',
                           'web.views.index',
                           name='index'),
                       # Url to change the user profile of the logged user.
                       url(r'^user_profile/change',
                           'web.views.user_profile_change',
                           name='user_profile_change'),
                       # Url to show the user profile of the logged user.
                       url(r'^user_profile',
                           'web.views.user_profile',
                           name='user_profile'),
                       # Url to show legal notes.
                       url(r'^legal_notes',
                           'web.views.legal_notes',
                           name='legal_notes'),
                       # Url to show credits.
                       url(r'^credits',
                           'web.views.show_credits',
                           name='show_credits'),
                       # Welcome.
                       url(r'^accounts/success/$',
                           'web.views.success',
                           name='success'),
                       # Remove the logout confirmation step.
                       url(r'^accounts/logout/$',
                           'django.contrib.auth.views.logout',
                           {'next_page': '/'}),
                       # Django registration urls.
                       url(r'^accounts/',
                           include('allauth.urls')),
                       # Create a new query.
                       url(r'^explorer/new/$', CreateQueryView.as_view(),
                           name='query_create'),
                       # In order to copy a public query in the personal one.
                       url(r'^explorer/copy/$',
                           'web.views.query_copy',
                           name='query_copy'),
                       # Override django sql explorer with a custom view
                       # with custom form with custom validators.
                       url(r'^explorer/(?P<query_id>\d+)/$',
                           QueryView.as_view(),
                           name='query_detail'),
                       # Recent queries.
                       url(r'^explorer/recent_queries/$',
                           'web.views.recent_queries',
                           name='recent_queries'),
                       # List the django explorer query using a custom view.
                       url(r'^explorer/$',
                           'web.views.query_list',
                           name='query_list'),
                       # Override explorer delete.
                       url(r'(?P<pk>\d+)/delete$',
                           'web.views.delete_query',
                           name='query_delete'),
                       # Django explorer urls.
                       url(r'^explorer/',
                           include('explorer.urls')),
                       )
