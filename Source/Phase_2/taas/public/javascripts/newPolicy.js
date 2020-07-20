var rowId = 1;
var isNewPolicy = true;
//var token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhZG1pbiIsImV4cCI6MTUwOTk2OTk2OTI3Nn0.SoBp5IQJAUCZUgTK203noGevp-cjrq1h7-gOgOCAejw'
var token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhZG1pbiIsImV4cCI6MTUxNzQ2NDk4MTU1OH0.NXiEgYOi8DDWAtJK6RbNTN4bPDtdn7bRHGTdcZdTJlg';


//get existing policies if any
$('.txtPolicyName').on('input',function(){
        var policyKey = $('.txtPolicyName').val();
        console.log("The text has been changed.",policyKey);

        $.ajax({
        type: "GET",
        url: "/api/getPolicy/"+policyKey,
        headers: {
            'x-access-token':token,
            'Content-Type':'application/json'
        },
        cache: false,
        success: function(response) {
            console.log(response);

            if(response != null 
                && response.object != null 
                && !$.isEmptyObject(response.object)
                && !$.isEmptyObject(response.object.columns)){

                    var i=0;
                    rowId = 1;
                    //clear previous table data if any
                    $('.table-Rules').find("tr:not(:first)").remove();
                    for(var key in response.object.columns){
                        $('.table-Rules tr:last').after('\
                            <tr>                                                                \n\
                                <td><input id="checkbox'+rowId+'" type="checkbox"></td>                                \n\
                                <td><input class="form-control" id="colName'+rowId+'" type="text" readonly="true" value='+key+'></td>\n\
                                <td>                                                            \n\
                                    <select class="form-control rule'+i+'" id="dropdown'+rowId+'">                               \n\
                                        <option value="0">Select</option>                       \n\
                                        <option value="1">mask</option>                         \n\
                                        <option value="2">hash</option>                         \n\
                                        <option value="3">range</option>                        \n\
                                        <option value="4">default</option>                      \n\
                                        <option value="5">default</option>                      \n\
                                        <option value="6">redact</option>                       \n\
                                        <option value="7">order</option>                        \n\
                                        <option value="8">irreversible</option>                 \n\
                                        <option value="9">ignoreFormat</option>                 \n\
                                        <option value="10">forget</option>                      \n\
                                    </select>                                                   \n\
                                </td>                                                           \n\
                            </tr>                                                               \n\
                        ');
                        $(".rule"+i).prop('selectedIndex', response.object.columns[key]);
                        i++;
                        rowId++;
                    }
                    
                    isNewPolicy = false;

                    $('.rulesTable').show();
                    $('.noRules').hide();

            }
            else{
                isNewPolicy = true;
                $('.rulesTable').hide();
                $('.noRules').show();
            }
        },
        error: function(err) {
            console.log(err);
        }

    });
        
});

//delete a policy
$('.btn-deleteRule').on('click', function(){
    swal({
        title: "Are you sure?",
        text: "Your will not be able to recover this imaginary file!",
        type: "warning",
        showCancelButton: true,
        confirmButtonClass: "btn-danger",
        confirmButtonText: "Yes, delete it!",
        closeOnConfirm: false
    },

    function(){
        var policyKey = $('.txtPolicyName').val();
        if(policyKey != undefined 
            && policyKey != null 
            && policyKey != ""){
            $.ajax({
                type: "DELETE",
                url: "/api/deletePolicy/"+policyKey,
                cache: false,
                headers: {
                    'x-access-token':token,
                    'Content-Type':'application/json'
                    },
                success: function(response) {
                    console.log("success",response);
                    swal(response.message);
                    //clear previous table data if any
                    $('.table-Rules').find("tr:not(:first)").remove();
                    var i=0;
                    rowId = 1;
                    $('.txtPolicyName').val("");
                    $('.rulesTable').hide();
                    $('.noRules').show();
                    isNewPolicy = true;
                },
                error: function(err) {
                    console.log(err);
                }

            });
        }
        //swal("Deleted!", "Your imaginary file has been deleted.", "success");
    });
});

//add new policy 
$('.btn-addNewRule').on('click', function (){

    //clear previous table data if it is a new policy
    if(isNewPolicy){
        $('.table-Rules').find("tr:not(:first)").remove();
        rowId = 1;
    }    
           
        $('.table-Rules tr:last').after('\
                <tr>                                                                \n\
                    <td><input type="checkbox" id="checkbox'+rowId+'"></td>                                \n\
                    <td><input class="form-control" id="colName'+rowId+'" type="text" placeholder="Column Name"></td>\n\
                    <td>                                                            \n\
                        <select class="form-control" id="dropdown'+rowId+'">                               \n\
                            <option value="0">Select</option>                       \n\
                            <option value="1">mask</option>                         \n\
                            <option value="2">hash</option>                         \n\
                            <option value="3">range</option>                        \n\
                            <option value="4">default</option>                      \n\
                            <option value="5">default</option>                      \n\
                            <option value="6">redact</option>                       \n\
                            <option value="7">order</option>                        \n\
                            <option value="8">irreversible</option>                 \n\
                            <option value="9">ignoreFormat</option>                 \n\
                            <option value="10">forget</option>                      \n\
                        </select>                                                   \n\
                    </td>                                                           \n\
                </tr>                                                               \n\
        ');
        rowId++;

        $('.rulesTable').show();
        $('.noRules').hide();
    
    });

$('.createPolicy').on('click', function(){
    console.log('into create policy');
    var columns = {};
    if(rowId > 1){
        for(var j=1;j<rowId;j++){
            //var obj = {};
            var colName = $('#colName'+j).val();
            columns[colName.toString()] = parseInt($('#dropdown'+j).val());
        }
    }
    console.log('columns',columns);

    var request = {};
    request["policy"] = {};
    request.policy["name"] = $('.txtPolicyName').val();
    request.policy["columns"] = columns;
    //hard coring data temporarily
    request.policy.metadata={};
    request.policy.metadata["user_id"] = "Laasya";
    request.policy.metadata["created_time"] = moment().format('MM-DD-YYYY HH:mm:ss');
    request.policy.metadata["last_executed_time"] = moment().format('MM-DD-YYYY HH:mm:ss');
    request.policy.metadata["source"] = "";
    console.log("request",request, JSON.stringify(request));
    $.ajax({
         type: "POST",
         url: "/api/updatePolicy",
         cache: false,
         headers: {
            'x-access-token':token,
            'Content-Type':'application/json'
            },
         data: JSON.stringify(request),
         success: function(response) {
            console.log("success",response);
            swal(response.message);
         },
         error: function(err) {
             console.log(err);
         }

     });

    
});

function loadRules(){
    console.log('load rules csv');
    var regex = /^([a-zA-Z0-9\s_\\.\-:])+(.csv|.txt)$/;
    if (regex.test($(".fileUpload").val().toLowerCase())){
        if (typeof (FileReader) != "undefined"){
            var reader = new FileReader();
            reader.onload = function (e){
                var rows = e.target.result.split("\n");
                console.log('rows', rows);
                bindTable(rows);
                // for (var i = 0; i < rows.length; i++) {
                //     var cells = rows[i].split(",");
                //     console.log('cells',cells);
                // }
            }
            reader.readAsText($(".fileUpload")[0].files[0]);
        }
        else {
            alert("This browser does not support HTML5.");
        }
        
    }
    else {
        alert("Please upload a valid CSV file.");
    }
}

function bindTable(data){
    //clear previous table data if any
    $('.table-Rules').find("tr:not(:first)").remove();
    var i=0;
    rowId = 1;

    for(var k=0;k<data.length;k++){
        if(data[k] != null && data[k] != "" && data[k].toLowerCase().indexOf("columns") == -1){
            var cells = data[k].split(",");

            $('.table-Rules tr:last').after('\
                <tr>                                                                \n\
                    <td><input id="checkbox'+rowId+'" type="checkbox"></td>                                \n\
                    <td><input class="form-control" id="colName'+rowId+'" type="text" readonly="true" value='+cells[0]+'></td>\n\
                    <td>                                                            \n\
                        <select class="form-control rule'+i+'" id="dropdown'+rowId+'">                               \n\
                            <option value="0">Select</option>                       \n\
                            <option value="1">mask</option>                         \n\
                            <option value="2">hash</option>                         \n\
                            <option value="3">range</option>                        \n\
                            <option value="4">default</option>                      \n\
                            <option value="5">default</option>                      \n\
                            <option value="6">redact</option>                       \n\
                            <option value="7">order</option>                        \n\
                            <option value="8">irreversible</option>                 \n\
                            <option value="9">ignoreFormat</option>                 \n\
                            <option value="10">forget</option>                      \n\
                        </select>                                                   \n\
                    </td>                                                           \n\
                </tr>                                                               \n\
            ');

            if(cells.length > 1 && cells[1] != null && cells[1] != ""){
                $(".rule"+i).prop('selectedIndex', cells[1]);
            }
            i++;
            rowId++;
        }
    }
    $('.rulesTable').show();
    $('.noRules').hide();

}

