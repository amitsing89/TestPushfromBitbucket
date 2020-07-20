var token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhZG1pbiIsImV4cCI6MTUxNzQ2NDk4MTU1OH0.NXiEgYOi8DDWAtJK6RbNTN4bPDtdn7bRHGTdcZdTJlg';
var policyData = [];
// datatable = $("#policies-table").dataTable({
//             "bPaginate": true,
//             "aLengthMenu": [
//                 [10, 25, 50, -1],
//                 [10, 25, 50, "All"]
//             ],
//             "bFilter": true,
//             "bSort": true,
//             "order": [
//                 [1, "asc"]
//             ],
//             "bInfo": true,
//             "bAutoWidth": false,
//             "bDeferRender": true,
//             "aaData": [],
//             "bDestroy": true,
//             "aoColumnDefs": [{
//                 'bSortable': false,
//                 'aTargets': [0, -1]
//             }],
//             "aoColumns": [{
//                     "mData": null,
//                     "mRender": function(data, type, full) {
//                         return '<input type="checkbox" />';
//                     }
//                 }, {
//                     "mData": "ID_NUM",
//                     "sDefaultContent": ""
//                 }, {
//                     "mData": "TEAM",
//                     "sDefaultContent": ""
//                 }, {
//                     "mData": "CONTROL_PROCESS",
//                     "sDefaultContent": ""
//                 }, {
//                     "mData": "SYSTEM",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "ALERT_TYPE",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "MIN_ERROR_DATE",
//                     "mRender": function(data, type, full) {
//                         return data != null ? moment(data).format('DD/MMM/YYYY') : "";
//                     }
//                 }, {
//                     "mData": "STATUS",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "AG",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "STREAM",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "ASSIGNED_TO",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": "LATESTUPDATEDATE",
//                     "mRender": function(data, type, full) {
//                         return data != null ? data : "";
//                     }
//                 }, {
//                     "mData": "LASTUPDATEDBY",
//                     "sDefaultContent": " "
//                 }, {
//                     "mData": null,
//                     "mRender": function(data, type, full) {
//                         return actionDiv;
//                     }
//                 },

//             ]
//         });



function getAllPolicies(){
    $.ajax({
         type: "GET",
         url: "/api/getPolicyKeys",
         cache: false,
         headers: {
            'x-access-token':token,
            'Content-Type':'application/json'
            },
         success: function(response) {
            console.log("success",response);
            if(response != null 
                && response.keys != null 
                && response.keys.length != 0){


                    response.keys.forEach(function(policyKey){
                        $.ajax({
                            type: "GET",
                            url: "/api/getPolicy/"+policyKey,
                            headers: {
                                'x-access-token':token,
                                'Content-Type':'application/json'
                            },
                            cache: false,
                            success: function(data) {
                                console.log(data);

                                if(data != null 
                                    && data.object != null 
                                    && !$.isEmptyObject(data.object)
                                    && !$.isEmptyObject(data.object.metadata)
                                    && !$.isEmptyObject(data.object.columns)){

                                    var obj = {};
                                    obj.name = policyKey;
                                    obj.createdBy = data.object.metadata.user_id;
                                    obj.created = data.object.metadata.created_time;
                                    obj.updated = data.object.metadata.last_executed_time;
                                    obj.cols = Object.keys(data.object.columns).length;
                                    policyData.push(obj);
                                    if(policyData.length == response.keys.length){
                                        console.log('policyData',policyData);
                                        datatable = $("#policies-table").dataTable();
                                        datatable.fnClearTable();
                                        datatable.fnAddData(policyData);
                                        datatable.fnDraw();
                                    }
                                }
                                else{
                                    
                                }
                            },
                            error: function(err) {
                                console.log(err);
                                swal(err);
                            }
                        });
                    });                    
                }                
         },
         error: function(err) {
             console.log(err);
             swal(err);
         }

     });
}