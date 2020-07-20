


var DBAdmin = function (){
    
    
    var handleTable = function () {
        
        $('.table-listPolics').DataTable({
            
            "aoColumnDefs": [
                { 'bSortable': false, 'aTargets': [ 6 ] }
            ],
            "aoColumns":[{
                "mData": "name",
                "sDefaultContent": ""
            },{
                "mData": "cols",
                "sDefaultContent": ""
            },{
                "mData": "createdBy",
                "sDefaultContent": ""
            },{
                "mData": "created",
                "mRender": function(data, type, full) {
                        return data != null ? moment(data).format('DD/MMM/YYYY') : "";
                    }
            },{
                "mData": "updated",
                "mRender": function(data, type, full) {
                        return data != null ? moment(data).format('DD/MMM/YYYY') : "";
                    }
            },{
                "mData": null,
                "mRender": function(data, type, full) {
                    return '<label class="label label-success">Active</label>';
                }
            },
            {
                "mData": null,
                "mRender": function(data, type, full) {
                    return '<a class="btn btn-danger btn-sm"><i class="zmdi zmdi-delete"></i></a>'+
                        '<a class="btn btn-info btn-sm" data-toggle="modal" data-target="#mdlViewRule"><i class="zmdi zmdi-eye"></i></a>'+
                        '<a class="btn btn-success btn-sm"><i class="zmdi zmdi-check"></i></a>';
                }
            }],
            
            "oLanguage": {
                "sLengthMenu": "_MENU_ Rows",
                "sSearch": ""                
            },
            
            "aLengthMenu": [
                [5, 10, 15, 20, 50, -1],
                [5, 10, 15, 20, 50, "All"] // change per page values here
            ],
            
            "order": [[ 4, "desc" ]],
            
            "iDisplayLength": 10    // set the initial value
        });
       
        $('.dataTables_filter input').attr("placeholder", "Search...");
        
    };
    
    return {
        init: function() {
            handleTable();
        }
        
    };
}();
