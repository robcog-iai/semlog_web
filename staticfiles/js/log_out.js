$(document).ready(function() {

    $.ajaxSetup({
        data: { csrfmiddlewaretoken: token },
    });

    function log_out() {
        $.ajax({
            type: "POST",
            data: {},
            url: "/log_out/",
            cache: false,
            dataType: "json",
            success: function(result, statues, xml) {
                console.log(result)
            },
            async: false
                // error: function(xhr, status, error) {
                //     alert(xhr.responseText);
                // },
        });
        return false;
    }

    $(".log_out").click(function() {
        log_out()
        window.location.href = "/"
    })
})