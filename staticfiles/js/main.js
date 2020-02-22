var r;

var flag_start_reading = 0
$(document).ready(function() {

    $("#query_input").focusout(function() {

        if (this.value.length > 0) {
            this.style.width = ((this.value.length + 1) * 9) + 'px';
        } else {
            this.style.width = ((this.getAttribute('placeholder').length + 1) * 8) + 'px';
        }

    });

    $.ajaxSetup({
        data: { csrfmiddlewaretoken: token },
    });

    var interval = 2000;


    function update_server_msg() {
        var log_info;
        $.ajax({
            type: "POST",
            data: {},
            url: "/read_log/",
            cache: false,
            dataType: "json",
            success: function(result, statues, xml) {
                log_info = result
            },
            async: false,
            error: function(xhr, status, error) {
                console.log(error)
            },

        });
        return log_info

    }




    $("#search").click(function() {
        console.log(flag_start_reading)

        // $(".class_accord").removeClass("active")
        // $(".class_terminal").addClass("active")

        $(".operation_button").addClass("disabled")

        $(".training_button").addClass("disabled")
        var query_input = $("#query_input").val()
        var resize_input = $("#resize_input").val()

        $.ajax({
            type: "POST",
            data: {
                "query_input": query_input,
                "resize_input": resize_input
            },
            url: "/search_database/",
            cache: false,
            dataType: "json",
            success: function(result, statues, xml) {
                r = result;
            },
            async: false
                // error: function(xhr, status, error) {
                //     alert(xhr.responseText);
                // },
        });
        if (flag_start_reading == 0) {
            var interval = window.setInterval(function() {
                var flag_stop = 0
                var flag_classifier = 0
                var log_info = update_server_msg()
                log_info = log_info['data'].split("@")
                $("#server_log").empty()
                for (let key in log_info) {
                    var content = log_info[key]
                    var text = document.createTextNode(log_info[key])
                    $("#server_log").append(text)
                    $("#server_log").append("<br />")
                    if (content.includes("succeeded")) {
                        $(".operation_button").removeClass("disabled")
                        if (flag_classifier == 1) {
                            $(".training_button").removeClass("disabled")
                        }
                    }
                    if (content.includes("classifier")) {
                        flag_classifier = 1

                    }
                    if (content.includes("Query")) {
                        // flag_stop = 1
                    }
                }


                if (flag_stop == 1) {
                    clearInterval(interval)
                }

            }, 2000)
            return false;
        }



    })

    $("#search").click(function() {
        flag_start_reading = 1
    })



    function get_databases() {
        $.ajax({
            type: "POST",
            data: {},
            url: "/update_database_info/",
            cache: false,
            dataType: "json",
            success: function(result, statues, xml) {
                r = result;
            },
            async: false
                // error: function(xhr, status, error) {
                //     alert(xhr.responseText);
                // },
        });
        return false;
    }




    get_databases()

    var loop_counter = 0;
    var first_db_button;
    Object.keys(r).forEach(function(each_db) {
        var db_button = document.createElement("BUTTON")
        db_button.type = "button"
        db_button.classList.add("ui", "button")
        db_button.id = each_db
        db_button.innerHTML = each_db
        db_button.style.width = "100px"
        db_button.style.height = "40px"
        db_button.style.marginTop = "10px"
        $("#database_list").append(db_button)
        if (loop_counter == 0) {
            first_db_button = each_db
        }
        loop_counter += 1

        $("#" + each_db).click(function() {
            $("#detail_list").empty()

            details = r[each_db]

            var task_title = document.createElement("h4")
            task_title.innerHTML = "Task Description:"
            var task = document.createElement("p")
            task.style.marginLeft = "20px"
            task.innerHTML = details['task_description']
            $("#detail_list").append(task_title)
            $("#detail_list").append(task)

            var collection_title = document.createElement("h4")
            collection_title.innerHTML = "Collections:"
            var collections = document.createElement("p")
            collections.style.marginLeft = "20px"
            collections.innerHTML = details['collections'].join(", ")
            $("#detail_list").append(collection_title)
            $("#detail_list").append(collections)

            var entities_title = document.createElement("h4")
            entities_title.innerHTML = "Entities:"
            entities_info = details['entities']
            var entities = document.createElement("p")
            entities.style.marginLeft = "20px"
            for (var each_class in entities_info) {
                id_array = entities_info[each_class]
                id_content = id_array.join(", ")
                class_elem = document.createElement("h5")
                class_elem.innerHTML = each_class + " -->"
                entities.append(class_elem)
                id_elem = document.createElement("p")
                id_elem.style.marginLeft = "20px"
                id_elem.innerHTML = id_content
                entities.append(id_elem)
            }

            $("#detail_list").append(entities_title)
            $("#detail_list").append(entities)


            var skels_title = document.createElement("h4")
            skels_title.innerHTML = "Skeletal Entities:"
            bones_info = details['skels']
            var skels = document.createElement("p")
            skels.style.marginLeft = "20px"
            for (var each_skel in bones_info) {
                bone_array = bones_info[each_skel]
                bone_content = bone_array.join(", ")
                skel_elem = document.createElement("h5")
                skel_elem.innerHTML = each_skel + " -->"
                skels.append(skel_elem)
                bone_elem = document.createElement("p")
                bone_elem.style.marginLeft = "20px"
                bone_elem.innerHTML = bone_content
                skels.append(bone_elem)
            }

            $("#detail_list").append(skels_title)
            $("#detail_list").append(skels)

            var camera_view_title = document.createElement("h4")
            camera_view_title.innerHTML = "Camera Views:"
            var camera_view = document.createElement("p")
            camera_view.style.marginLeft = "20px"
            camera_view.innerHTML = details['camera_views'].join(", ")
            $("#detail_list").append(camera_view_title)
            $("#detail_list").append(camera_view)

        })

    })
    $("p").css("marginTop", "10px")
    $("p").css("marginBottom", "10px")
    $("h4").css("marginTop", "10px")
    $("h4").css("marginBottom", "10px")
    $("h5").css("marginTop", "10px")
    $("h5").css("marginBottom", "10px")



    $("#" + first_db_button).trigger('click')

    // $('#main_form').on('keyup keypress', function(e) {
    //     var keyCode = e.keyCode || e.which;
    //     if (keyCode === 13) {
    //         e.preventDefault();
    //         return false;
    //     }
    // });



})