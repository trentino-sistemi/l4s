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
  Query editor customization popup
  This contains all the javascript used by the query editor customization popup.
*/


function get_aggregation_color() {
        return "#5bc0de";
}

function colour(field) {
        element = document.getElementById(field);
        element.setAttribute("style", "background:" + get_aggregation_color());
    }

function load_selected_values(myRadio, field, values, agg){
    var array = eval('(' + values + ')');
    id = "select_" + field;
    var radio = document.getElementById(id);
    var html = "";
    for (var i = 0; i < array.length; i++) {
        if (agg) {
            id =  'agg_input_'  + myRadio.id +'_' + array[i][0]
            name = 'agg_input_' + myRadio.id;
            radio.name =  'select_input_' + myRadio.id;
        }
        else {
            id =  field + '_input_' + array[i][0]
            name = 'input_' +  field;
        }
        html += '<input type="checkbox" name="' +  name + '" id="' + id + '" value="' + array[i][0] + '">'
        if (array[i].length>1) {
            html+= array[i][1] + '<br>';
        }
        else {
             html+= array[i][0] + '<br>';
        }
    }
    radio.innerHTML = html;
}

function change_field_label(myRadio, field) {
    html = myRadio.value;
    var span = document.getElementById(field);
    var spans = span.getElementsByTagName("span");
    html = html + '<span style="display:None">' + spans[0].innerHTML + '</span>';
    html = html + '<span class="caret"></span>';
    span.innerHTML = html;
}

function handleRadio(myRadio, field, values, agg, select_all) {
    change_field_label(myRadio, field);
    load_selected_values( myRadio, field, values, agg);
    if (select_all) {
        checkByParent('select_'+ field, true);
    }
}

function setFieldLabel(id, field, values) {
   var myRadio = document.getElementById(id);
   handleRadio(myRadio, field, values, true, false);
}

function get_lis(id) {
    ret = ""
    element = document.getElementById(id);
    var lis = element.getElementsByTagName('li');
    var output = [];
    for (var i = 0; i < lis.length; i++) {
         var span = lis[i].getElementsByTagName("span")[0];
         output.push(span.getElementsByTagName("span")[0].innerHTML);
    }
    return output.join(",");
}

function get_aggregations() {
    col_el = document.getElementById('columnFields');
    row_el = document.getElementById('rowFields');
    
    var radios = document.getElementsByTagName('input');
    var output = [];
    for (var i = 0; i < radios.length; i++) {
        var name = radios[i].name;
        var type = radios[i].type;
        if (type == 'radio' && radios[i].checked) {
            id = radios[i].id;
            if (id != "") {
                output.push(id);
           }
       }
    }
    return output.join(",");
}

function checkByParent(aId, aChecked) {
    el = document.getElementById(aId);
    var coll= el.getElementsByTagName('input');
    for (var x=0; x<coll.length; x++) {
        coll[x].checked = aChecked;
    }
}

function create_selection(values, too_many) {
    var selection_obj = new Object();
    var value_hash = eval('(' + values + ')');
    for (var key in value_hash) {
        field_obj = [];
        selection_obj[key] = field_obj
        field_value = value_hash[key];  
        sel_name = "input_" + key;
        var coll = document.getElementsByName(sel_name);
        sp = document.getElementById(key);
        if (sp == null ) {
            continue;
        }
        cl = sp.parentNode.parentNode.getAttribute("id");
        sel_ref_period_count = 0;
        if (coll != null) {
             for (var x=0; x<coll.length; x++) {
                if (coll[x].checked) {
                    ref_period = coll[x].getAttribute("ref_period");
                    if (ref_period  != null  && ref_period == "true"){
                       if (cl == "unselectedFields" ) {
                           sel_ref_period_count += 1;
                       }
                    }
                    if (sel_ref_period_count > 1) {
                         name = sp.getAttribute("name");
                        alert(name + "; " + too_many);
                        return null;
                    }
                    field_obj.push(field_value[x]);
                }        
             }
        }   
    }    
   return selection_obj;
}

function create_agg_selection(agg_values) {
    var selection_obj = new Object();
    var value_hash = eval('(' + agg_values + ')');
    for (var key in value_hash) {
        field_obj = [];
        selection_obj[key] = field_obj
        field_value = value_hash[key];  
        sel_name = "agg_input_" + key;
        var coll = document.getElementsByName(sel_name);
             for (var x=0; x<coll.length; x++) {
                if (coll[x].checked) {
                    field_obj.push(field_value[x]);
                }
        }     
    }
   return selection_obj;
}

function create_obs_selection(obs_values) {
    obs_obj =  eval('(' + obs_values + ')');
    sel_obj = [];
    for (var i = 0; i < obs_obj.length; i++) {
       obs_value = obs_obj[i];
       id= "obs_" + obs_value;
       el = document.getElementById(id);
       if (el.checked) {
           sel_obj.push(obs_value);
       }
    }
    return sel_obj;
}

function select_filters(filters){
    var filter_hash = eval('(' + filters + ')');
    for (var key in filter_hash) {
        field_values = filter_hash[key]; 
        for (var x=0; x<  field_values.length; x++) {
            var val =  field_values[x][0];
            id = key + '_input_' + val
            el = document.getElementById(id);
            if (el != null) {
                el.checked = true;
            }
        }    
    }
}

function select_agg_filters(filters){
    var filter_hash = eval('(' + filters + ')');
    for (var key in filter_hash) {
        field_values = filter_hash[key]; 
        for (var x=0; x<  field_values.length; x++) {
            var val =  field_values[x][0];
            id= "agg_input_" + key + "_" + val;
            el = document.getElementById(id);
            if (el != null) {
                el.checked = true;
            }
        }    
    }
}

function addHiddenInput(form, id, value) {
    var hiddenField = document.createElement("input");
    hiddenField.setAttribute("type", "hidden");
    hiddenField.setAttribute("id", "id_" + id);
    hiddenField.setAttribute("name", id);
    hiddenField.setAttribute("value", value);
    form.appendChild(hiddenField);
}


function submit_popup (obs_values, values, agg_values, table_name, no_rows, no_columns, no_values, too_many) {
    selection = create_selection(values, too_many);
    if (selection == null) {
        return
    }
    
    filter_value = JSON.stringify(selection);
        
    rows = get_lis('rowFields');
    if (rows == "") {
        alert(no_rows);
        return;
    }
    cols = get_lis('columnFields');
    if (cols == "") {
        alert(no_columns);
        return;
    }
    
    selected_obs = create_obs_selection(obs_values) ;
    if (selected_obs.length==0) {
         alert(no_values);
        return;
    }
    selected_obs_values = selected_obs .join(",")
    
    sel_aggregations = get_aggregations();
    agg_selection = create_agg_selection(agg_values);
    agg_selection_value = JSON.stringify(agg_selection);
    debug_value = "false";
    var debug = document.getElementById('debug');  
      if (debug!=null && debug.checked == 1) {
        debug_value = "true";
     }
    
    include_code_value = "false"
    var include_code = document.getElementById('include_code');  
      if (include_code!=null && include_code.checked == 1) {
        include_code_value = "true";
     }
    
    url="/query_editor_view/"
    data = { 'table': table_name,
                      'include_code': include_code_value,
                      'columns': cols,
                      'rows': rows,
                      'selected_obs_values':  selected_obs_values,
                      'aggregate': sel_aggregations,
                      'filters': filter_value,
                      'agg_filters': agg_selection_value,
                      'debug': debug_value,
              };
    $.ajax({
		url: url,
        type: "POST",
        data: data,
        success: function(response) {
            window.opener.document.write(response);
            window.opener.document.close();
            window.close();
	    },
        error: function(xhr, status) {
			alert(xhr.responseText);
		}
    });
}

$(function () {
    $("#unselectedFields,#columnFields,#rowFields").sortable({
         connectWith: "#unselectedFields,#columnFields,#rowFields",
    });
    $("#unselectedFields,#columnFields,#rowFields").disableSelection();
});

$(function () {
   $('.dropdown-menu').on({
	"click":function(e){
      e.stopPropagation();
    }
   });
});
