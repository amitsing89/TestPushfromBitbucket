

;(function ($) {
    'use strict';
    
    
    //  Functions to handle Basic Bootstrap Elements
    $('[data-toggle="popover"]').popover();
    
    $('[data-toggle="tooltip"]').tooltip()
    
//    $(".toggle-fullscreen").on("click", function () {
//        $(document).toggleFullScreen();
//    });
    
    
    // function to Handel Slim Scroll
    if ($().slimScroll) {
        $(".scroller").each(function () {
            $(this).slimScroll({
                size: "4px",
                opacity: "0.5",
                position: $(this).attr("data-position"),
                height: $(this).attr("data-height"),
                alwaysVisible: ($(this).attr("data-always-visible") == "1" ? true : false),
                railVisible: ($(this).attr("data-rail-visible") == "1" ? true : false),
                disableFadeOut: true,
                color: 'rgba(0,0,0,.2)'
            });
        });
    }
    
    
    // function to Handel File Select
    if ($().filestyle) {
        $(":file").each(function () {
            $(this).filestyle({
                
            });
        });
    }
    
    
    // functions to handel the panel tools
    var panel_collapse = $('.panel .tools > .panel-collapse');
    var panel_reload = $('.panel .tools > .reload');
    var panel_expand = $('.panel .tools>.expand');
    var panel_close = $('.panel .tools>.panel-close');
    
    
    // For Panel Collapse and expend
    panel_collapse.on("click", document, function () {
        var panel_body = $(this).parent().closest(".panel").children(".panel-body");
        var panel_slim_scroll = $(this).parent().closest(".panel").children(".slimScrollDiv");

        if ($(this).hasClass("collapses")) {
            $(this).addClass("panel-expand").removeClass("collapses");
            panel_body.slideUp(200);
            panel_slim_scroll.slideUp(200);
        } else {
            $(this).addClass("collapses").removeClass("panel-expand");
            panel_body.slideDown(200);
            panel_slim_scroll.slideDown(200);
        }
    }); 
    
    
    // For Panel Reload
    panel_reload.on("click", document, function () {
        var panel = $(this).parents(".panel");
        panel.block({
            overlayCSS: {
                backgroundColor: '#FFFFFF'
            },
            message: '<strong>LOADING...</strong>',
            css: {
                border: 'none',
                color: '#333333',
                background: 'none'
            }
        });

        window.setTimeout(function () {
            panel.unblock();
        }, 3000);
    });
    
    
    //  For Panel full Screen
    panel_expand.on("click", document, function () {
        var panel = $(this).parents('.panel');
        var panel_tools = $(this).closest('.tools').find('a').not(this);
        panel.removeAttr('style');


        if (panel.hasClass('panel-full-screen')) {
            panel.removeClass('panel-full-screen');
            panel_tools.show();
        } else {
            panel_tools.hide();
            panel.addClass('panel-full-screen');
        }
    });
    
    
    //  For Panel Close
    panel_close.on("click", document, function () {
        $(this).parents(".panel").remove();
    }); 

    // function to Handel Select2
    if (jQuery().select2) {

        $(".select2").each(function () {
            $(this).select2({
                placeholder: 'Select an option',
                minimumResultsForSearch: -1
            });
        });


        $(".select2Search").each(function () {
            $(this).select2({
                placeholder: 'Select an option'
            });
        });


        $(".select2Tags").each(function () {
            $(this).select2({
                tags: "true",
                placeholder: "Select an option",
                allowClear: true
            });
        });

    }
    
    
    // function to Handel Character Counter
    if(jQuery().characterCounter) {
        $(".txtCharCounter").each(function() {
            $(this).characterCounter({
                maxLen: $(this).attr("maxlength"),
                warningLen: 5,
                showMsg: true,
                customMsg: "You have exceeded the permissible limit of characters..",
                separator: "/",
                warningColor: "#E03B30"
            });
        });
    }

    
    //Initialization of treeviews
    $(".list-tree").each(function () {
        $(this).treed({
            openedClass: $(this).attr('data-openedClass'),
            closedClass: $(this).attr('data-closedClass')
        });
    });
    
    
    // function to activate Flot Modals
    $(document).on('click', '[data-toggle="modal-float"]', function () {
        var target = $(this).data('target');
        $(target).toggleClass('open');
    });

    $(document).on('click', '[data-trigger="modal-float"]', function () {
        var target = $(this).data('target');
        $(target).addClass('open');
    });

    $(document).on('click', '[data-dismiss="modal-flot"]', function () {
        $(this).closest('.modal-flot').removeClass('open');
    });
    
    

})(jQuery);