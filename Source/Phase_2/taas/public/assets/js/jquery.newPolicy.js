


var NewPolicy = function (){
    
    
    var handleSelectPolicy = function () {
        
        var RulesName = new Bloodhound({
            datumTokenizer: Bloodhound.tokenizers.whitespace,
            queryTokenizer: Bloodhound.tokenizers.whitespace,
            prefetch: 'assets/demo/rules_names.json'
        });
        
        $('.txtPolicyName').typeahead(null, {
            name: 'RulesName',
            source: RulesName
        });

        
    };
    
    
    var handleReadOnly = function () {
        
        $('.table-Rules').on("focus", "input[type=text]", function() {
            $(this).attr("readonly", false);
        });
        
        $('.table-Rules').on("focusout", "input[type=text]", function() {
            $(this).attr("readonly", true);
        });
        
    };
    
    
    
    var handleAddNewRule = function() {
        
        $('.btn-addNewRule').on('click', function (){
           
            $('.table-Rules tr:last').after('\
                    <tr>                                                                \n\
                        <td><input type="checkbox"></td>                                \n\
                        <td><input class="form-control" type="text" readonly="true" placeholder="Column Name"></td>\n\
                        <td>                                                            \n\
                            <select class="form-control">                               \n\
                                <option>Lorem ipsum dolor sit amet</option>             \n\
                                <option>Consectetur adipiscing elit</option>            \n\
                                <option>Nullam posuere hendrerit tortor</option>        \n\
                                <option>Aenean eu magna in orci ullamcorper</option>    \n\
                                <option>Cras dapibus consequat</option>                 \n\
                                <option>Mauris vel blandit ex</option>                  \n\
                                <option>Ut consectetur posuere</option>                 \n\
                                <option>Vivamus enim orci</option>                      \n\
                                <option>Maecenas interdum lectus</option>               \n\
                                <option>Morbi ornare neque</option>                     \n\
                            </select>                                                   \n\
                        </td>                                                           \n\
                    </tr>                                                               \n\
            ');
        
        });
       
    };
    
    
    
    var handleRulesSelect = function() {
        
        $('.btn-deleteRule').css('display', 'none');
        
        $('.table-Rules').on("change", "input[type=checkbox]", function() {
            
            var numberOfChecked = $('.table-Rules input:checked').length;
            
            if(numberOfChecked > 0){
                $('.btn-deleteRule').css('display', 'inline-block');
            } else {
                $('.btn-deleteRule').css('display', 'none');
            }
            
            
            if ($(this).is(':checked')) {
                $(this).closest('tr').addClass('selected');
            }else{
                $(this).closest('tr').removeClass('selected');
            }
            
        });
        
    };
    
    
    
    var handleRemoveSeletedRules = function() {
        
        $('body').on("click", ".btn-deleteRule", function() {            
            
            var checkboxes = $('.table-Rules input[type=checkbox]');
            
            if ($(checkboxes).is(':checked')) {
                
                $('tr.selected').addClass('deleted');
                
            }
            
        });
        
    };
    
    
    
    return {
        init: function() {
            handleSelectPolicy();
            handleReadOnly();
            handleAddNewRule();
            handleRulesSelect();
            handleRemoveSeletedRules();
        }
        
    };
}();
