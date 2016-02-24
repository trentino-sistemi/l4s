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

function filtra_elementi(padre, elemento, nome_select) {

    var radios = document.getElementsByName(nome_select);
    var id_agg;
    for (var i = 0; i < radios.length; i++) {
        if (radios[i].type === 'radio' && radios[i].checked) {
            // get value, set checked flag or do whatever you need to
            id_agg = radios[i].value;
        }
    }

    var lista_valori = new Array();

    el = document.getElementById(elemento);
    var coll= el.getElementsByTagName('input');
    for (var x=0; x<coll.length; x++) {
        if (coll[x].checked == true) {
            lista_valori.push(coll[x].value);
        }
    }

    if (lista_valori.length == 0) {
        checkByParent(padre, false);
    }
    else {
        url="/get_list_of_value/"
        data = {'lista_valori': JSON.stringify(lista_valori),
                'id_agg': id_agg
               };

        $.ajax({
            url: url,
            type: "POST",
            data: data,
            success: function(response) {
                //alert(response);

                checkByParent(padre, false);

                lista = response.split(",");

                el = document.getElementById(padre);
                var coll= el.getElementsByTagName('input');
                for (var x=0; x<coll.length; x++) {

                    if (lista.indexOf(coll[x].value) > -1) {

                        coll[x].checked = true;
                    }

                }

                //document.write(response);
                //document.close();
        },
            error: function(xhr, status) {
                close_spinner(spinner, "modal");
                bootbox.alert(xhr.responseText);
            }
        });
    }
}

function load_selected_values(prefisso, myRadio, field, values, agg){

    id = prefisso + field;
    var radio = document.getElementById(id);
    var html = "";

    if (values != '') {
        var array = eval('(' + values + ')');

        for (var i = 0; i < array.length; i++) {
            if (agg) {
                id =  'agg_input_'  + myRadio.id +'_' + array[i][0]
                name = 'agg_input_' + myRadio.id;
                radio.name =  'select_input_' + myRadio.id;
            }
            else {
                if (prefisso == 'select_filtro_') {
                    id =  field + '_input_filtro_' + array[i][0]
                    name = 'input_filtro_' +  field;
                }
                else {
                    id =  field + '_input_' + array[i][0]
                    name = 'input_' +  field;
                }
            }
            html += '<input type="checkbox" name="' +  name + '" id="' + id + '" value="' + array[i][0] + '"';

            if (prefisso == 'select_filtro_')
                html += ' onclick="filtra_elementi(\'select_' + field + '\', \'select_filtro_' + field + '\', \'filtro_unused_' + field + '\');"';

            html += '>';


            if (array[i].length>1) {
                html+= array[i][1] + '<br>';
            }
            else {
                 html+= array[i][0] + '<br>';
            }
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
    load_selected_values('select_', myRadio, field, values, agg);
    if (select_all) {
        checkByParent('select_'+ field, true);
    }

    var li = document.getElementById("filtro_" + field);

    $('input[name=radio_grouped_by_' + field + ']').attr("disabled",agg);

    if (agg == true) {
        eventoonclick = '';
        $( ".dropdown-menu" ).removeClass( "attivo" );
    }
    else {
        eventoonclick = '$( ".dropdown-menu" ).toggleClass( "attivo" );';
    }

    li.onclick = new Function(eventoonclick);


}

function handleRadioFilter(myRadio, field, values, agg, select_all) {
  //alert(values)
  load_selected_values('select_filtro_', myRadio, field, values, agg);

  if (select_all) {
        checkByParent('select_filtro_'+ field, true);
  }

  checkByParent('select_'+ field, false);

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
         nome = span.getElementsByTagName("span")[0].innerHTML;

         if (nome.substring(0, 7) != 'filtro_') {
            output.push(span.getElementsByTagName("span")[0].innerHTML);
         }

    }
    return output.join(",");
}

function get_aggregations(in_riga_o_colonna) {
    var radios = document.getElementsByTagName('input');
    var output = [];
    for (var i = 0; i < radios.length; i++) {
        var name = radios[i].name;
        var type = radios[i].type;
        if (type == 'radio' && radios[i].checked && radios[i].getAttribute("grouped_by") == null) {

            var lista_id_parent_validi = new Array();

            if (in_riga_o_colonna == true) {
              lista_id_parent_validi.push('rowFields');
              lista_id_parent_validi.push('columnFields');
            }
              else
            {
              lista_id_parent_validi.push('unselectedFields');
            }

            id_parent = radios[i].parentNode.parentNode.parentNode.parentNode.parentNode.getAttribute("id");

            if (lista_id_parent_validi.indexOf(id_parent) != -1) {

              id = radios[i].id;
              if (id != "") {
                  output.push(id);
              }
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
        //alert(key);
        field_obj = [];
        selection_obj[key] = field_obj
        field_value = value_hash[key];
        //alert(field_value);
        sel_name = "input_" + key;
        var coll = document.getElementsByName(sel_name);
        sp = document.getElementById(key);
        cl = null;
        if (sp != null ) {
            cl = sp.parentNode.parentNode.getAttribute("id");
        }
        sel_ref_period_count = 0;
        if (coll != null) {
             for (var x=0; x<coll.length; x++) {
                if (coll[x].checked) {
                    ref_period = coll[x].getAttribute("ref_period");
                    if (ref_period  != null  && ref_period == "true"){
                       if (cl != null && cl == "unselectedFields" ) {
                           sel_ref_period_count += 1;
                       }
                    }
                    if (sel_ref_period_count > 1) {
                        name = sp.getAttribute("name");
                        bootbox.alert(name + "; " + too_many);
                        return null;
                    }
                    field_obj.push(field_value[x]);
                }        
             }
        }   
    }    
   return selection_obj;
}

function create_agg_selection(agg_values, sel_aggregations) {

    //alert("agg_values " + agg_values);

    var selection_obj = new Object();
    var value_hash = eval('(' + agg_values + ')');

    sel_aggregations_list = sel_aggregations.split(",");

    for (var key in value_hash) {

        if (sel_aggregations_list.indexOf(key) != -1) {

            field_obj = [];
            selection_obj[key] = field_obj
            field_value = value_hash[key];

            sel_name = "agg_input_" + key;

            //alert("key " + key);
            //alert("field_value " + field_value);
            //alert("sel_name " + sel_name);

            var coll = document.getElementsByName(sel_name);
                 for (var x=0; x<coll.length; x++) {
                    if (coll[x].checked) {
                        field_obj.push(field_value[x]);
                    }
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

    //alert(filters);

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

function get_grouped_by_in_query () {

    var radios = document.getElementsByTagName('input');
    var output = new Array;

    for (var i = 0; i < radios.length; i++) {
        var name = radios[i].name;
        var type = radios[i].type;

        if (type == 'radio' && radios[i].checked && radios[i].getAttribute("grouped_by") == "true") {

            //alert("name " + name);

            var lista_id_parent_validi = new Array();

            lista_id_parent_validi.push('rowFields');
            lista_id_parent_validi.push('columnFields');
            lista_id_parent_validi.push('unselectedFields');

            //alert("lista_id_parent_validi " + lista_id_parent_validi);

            id_parent = radios[i].parentNode.parentNode.parentNode.parentNode.parentNode.getAttribute("id");

            //alert("id_parent " + id_parent);

            if (lista_id_parent_validi.indexOf(id_parent) != -1) {

              id = radios[i].id;

              var elemento = {table_name:radios[i].getAttribute("table_name"), column_name:radios[i].getAttribute("column_name"), valore:id};

              //alert("id " + id);

              output.push(elemento);
            }
        }
    }

    return output;

}

function submit_popup (obs_values,
                       values,
                       agg_values,
                       table_name,
                       no_rows,
                       no_columns,
                       no_values,
                       too_many) {
						   
    selected_obs = create_obs_selection(obs_values);
    if (selected_obs == "") {
        bootbox.alert(no_values);
        return;
    }
    cols = get_lis('columnFields');
    if (cols == "") {
        bootbox.alert(no_columns);
        return;
    }
	rows = get_lis('rowFields');
    if (rows == "") {
        bootbox.alert(no_rows);
        return;
    }
    selection = create_selection(values, too_many);

    //alert(cols)

    if (selection == null) {
        return
    }
    spinner = $('#wrap').spin("modal");
    filter_value = JSON.stringify(selection);
    selected_obs_values = selected_obs.join(",")

    //Campi aggregati in riga o colonna
    sel_aggregations = get_aggregations(true); // questa e' ok
    //alert("sel_aggregations " + sel_aggregations);

    agg_selection = create_agg_selection(agg_values, sel_aggregations);  // questa e' ok
    //alert("agg_selection " + agg_selection);

    agg_selection_value = JSON.stringify(agg_selection);
    //alert("agg_selection_value " + agg_selection_value);

    //Campi aggregati nei campi NON selezionati
    not_sel_aggregations = get_aggregations(false); // questa e' ok
    //alert("not_sel_aggregations " + not_sel_aggregations);

    not_agg_selection = create_agg_selection(agg_values, not_sel_aggregations);  // questa e' ok
    //alert("not_agg_selection " + not_agg_selection);

    not_agg_selection_value = JSON.stringify(not_agg_selection);
    //alert("not_agg_selection_value " + not_agg_selection_value);

    get_grouped_by = get_grouped_by_in_query();
    get_grouped_by_value = JSON.stringify(get_grouped_by);
    //alert(get_grouped_by_value);

    debug_value = "false";
    var debug = document.getElementById('debug');  
      if (debug != null && debug.checked == 1) {
        debug_value = "true";
     }
    range_value = "false";
    var range = document.getElementById('range');
      if (range != null && range.checked == 1) {
        range_value = "true";
     }

    visible_value = "false";
    var visible = document.getElementById('visible');  
      if (visible != null && visible.checked == 1){
        visible_value = "true";
      }
    localStorage.setItem('visible', visible_value);
    
    include_code_value = "false"
    var include_code = document.getElementById('include_code');  
      if (include_code!=null && include_code.checked == 1) {
        include_code_value = "true";
     }
    $('#popup').modal('hide');
    url="/query_editor_view/"

    //alert(cols);
    //alert(rows);
    //alert(selected_obs_values);

    data = { 'table': table_name,
             'include_code': include_code_value,
             'columns': cols,                                    //fields in colonna
             'rows': rows,                                       //fields in riga
             'selected_obs_values':  selected_obs_values,        //fields in obs value
             'aggregate': sel_aggregations,                      //id delle aggregazioni di fields in riga o colonna
             'filters': filter_value,                            // tutti i fields .... quelli raggruppati sono vuoti []
             'agg_filters': agg_selection_value,                 //valore degli id delle aggregazioni es. (comunita di valle x , ccomunita di valle y ......)
             'debug': debug_value,
             'range': range_value,
             'visible': visible_value,
             'not_sel_aggregations': not_sel_aggregations,       //id delle aggregazioni di fields NON in riga o colonna
             'not_agg_selection_value': not_agg_selection_value,  ////valore degli id delle aggregazioni es. (comunita di valle x , ccomunita di valle y ......)
             'get_grouped_by_value': get_grouped_by_value
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

function includi_escludi (elemento) {

    //alert(elemento.checked);

    if (elemento.checked == true) {

        if (elemento.id == 'debug') {
            document.getElementById('visible').checked = false;
        }
          else
        {
            document.getElementById('debug').checked = false;
        }

    }

}
