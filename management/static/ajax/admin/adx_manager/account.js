/**
 * Reference Ajax AdX Account Data
 */

$().ready(function () {
    report_eror = function (jqXHR, exception) {
        var msg = '';
        if (jqXHR.status === 0) {
            msg = 'TIDAK ADA KONEKSI.\n TOLONG HUBUNGI DEVELOPER';
        } else if (jqXHR.status == 404) {
            msg = 'Requested page not found. [404]';
        } else if (jqXHR.status == 500) {
            msg = 'Internal Server Error [500].';
        } else if (exception === 'parsererror') {
            msg = 'Requested JSON parse failed.';
        } else if (exception === 'timeout') {
            msg = 'Time out error.';
        } else if (exception === 'abort') {
            msg = 'Ajax request aborted.';
        } else {
            msg = 'Uncaught Error.\n' + jqXHR.responseText;
        }
        alert(msg);
    };
    
    $('#btn_load_data').click(function (e) {
        load_adx_account_data();
    });
    
    // Auto load data on page load
    load_adx_account_data();
});

function load_adx_account_data() {
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_account',
        type: 'GET',
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            $("#overlay").hide();
            
            if (response.status) {
                // Update account information
                if (response.data) {
                    $("#network_id").text(response.data.network_id || '-');
                    $("#network_code").text(response.data.network_code || '-');
                    $("#display_name").text(response.data.display_name || '-');
                    $("#timezone").text(response.data.timezone || '-');
                    $("#currency_code").text(response.data.currency_code || '-');
                    
                    // Update account details section
                    var detailsHtml = '<div class="row">';
                    
                    if (response.data.network_id) {
                        detailsHtml += '<div class="col-md-6">';
                        detailsHtml += '<h6><i class="bi bi-info-circle text-primary"></i> Network Details</h6>';
                        detailsHtml += '<ul class="list-unstyled">';
                        detailsHtml += '<li><strong>Network ID:</strong> ' + response.data.network_id + '</li>';
                        detailsHtml += '<li><strong>Network Code:</strong> ' + response.data.network_code + '</li>';
                        detailsHtml += '<li><strong>Display Name:</strong> ' + response.data.display_name + '</li>';
                        detailsHtml += '</ul>';
                        detailsHtml += '</div>';
                    }
                    
                    if (response.data.timezone) {
                        detailsHtml += '<div class="col-md-6">';
                        detailsHtml += '<h6><i class="bi bi-clock text-success"></i> Configuration</h6>';
                        detailsHtml += '<ul class="list-unstyled">';
                        detailsHtml += '<li><strong>Timezone:</strong> ' + response.data.timezone + '</li>';
                        detailsHtml += '<li><strong>Currency:</strong> ' + response.data.currency_code + '</li>';
                        detailsHtml += '<li><strong>Last Updated:</strong> ' + new Date().toLocaleString() + '</li>';
                        detailsHtml += '</ul>';
                        detailsHtml += '</div>';
                    }
                    
                    detailsHtml += '</div>';
                    
                    // Add additional information if available
                    if (response.additional_info) {
                        detailsHtml += '<hr>';
                        detailsHtml += '<h6><i class="bi bi-plus-circle text-info"></i> Additional Information</h6>';
                        detailsHtml += '<div class="alert alert-info">';
                        detailsHtml += '<small>' + JSON.stringify(response.additional_info, null, 2) + '</small>';
                        detailsHtml += '</div>';
                    }
                    
                    $("#account_details").html(detailsHtml);
                    
                    // Show success message
                    showSuccessMessage('Account data loaded successfully!');
                } else {
                    $("#account_details").html('<p class="text-warning">No account data available.</p>');
                }
            } else {
                $("#account_details").html('<p class="text-danger">Error loading account data: ' + response.error + '</p>');
                alert('Error: ' + response.error);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            $("#account_details").html('<p class="text-danger">Failed to load account data. Please try again.</p>');
            report_eror(jqXHR, textStatus);
        }
    });
}

function showSuccessMessage(message) {
    // Create and show a temporary success alert
    var alertHtml = '<div class="alert alert-success alert-dismissible fade show" role="alert">';
    alertHtml += '<i class="bi bi-check-circle"></i> ' + message;
    alertHtml += '<button type="button" class="close" data-dismiss="alert" aria-label="Close">';
    alertHtml += '<span aria-hidden="true">&times;</span>';
    alertHtml += '</button>';
    alertHtml += '</div>';
    
    // Insert at the top of the card body
    $('.card-body').first().prepend(alertHtml);
    
    // Auto-hide after 3 seconds
    setTimeout(function() {
        $('.alert-success').fadeOut('slow', function() {
            $(this).remove();
        });
    }, 3000);
}

function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

const csrftoken = getCookie('csrftoken');