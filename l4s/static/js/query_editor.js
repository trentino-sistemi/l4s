/*
   This file is part of Lod4stat.

   Copyright (c) 2014 Provincia autonoma di Trento


   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

/*
  Query editor javascript ajax call.
*/

function query_editor(table,
                      columns,
                      rows,
                      obs_values,
                      include_code,
                      aggregations,
                      filters,
                      agg_filters,
                      range,
                      not_sel_aggregations,
                      not_agg_selection_value) {
    spinner = $('#wrap').spin("modal");
    include_code_value = "false"
    if (include_code=='True') {
       include_code_value = "true"
    }

    url="/query_editor_view/"
    data = { 'table': table,
             'columns': columns,
             'rows': rows,
             'selected_obs_values': obs_values,
             'aggregate': aggregations,
             'filters': filters,
             'agg_filters': agg_filters,
             'debug': "False",
             'include_code': include_code_value,
             'range': range,
             'not_sel_aggregations': not_sel_aggregations,
             'not_agg_selection_value': not_agg_selection_value
              };
    $.ajax({
		url: url,
        type: "POST",
        data: data,
        success: function(response) {
            document.write(response);
            document.close();
	    },
        error: function(xhr, status) {
            close_spinner(spinner, "modal");
			bootbox.alert(xhr.responseText);
		}
    });
}
