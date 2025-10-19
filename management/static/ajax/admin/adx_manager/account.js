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
    
    $('#btn_generate_refresh_token').click(function (e) {
        e.preventDefault();
        generateRefreshToken();
    });
    
    $('#btn_oauth_setup').click(function (e) {
        e.preventDefault();
        // Auto-fill user mail from current user data
        var currentUserMail = $('#user_mail').text();
        if (currentUserMail && currentUserMail !== '-') {
            $('#user_mail').val(currentUserMail);
        }
        $('#oauthModal').modal('show');
    });
    
    $('#btn_save_oauth').click(function (e) {
        e.preventDefault();
        saveOAuthCredentials();
    });
    
    // Auto load data on page load
    load_adx_account_data();
});

function load_adx_account_data() {
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_user_account',
        type: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function (response) {
            $("#overlay").hide();
            console.log(response)
            if (response && response.status) {
                // Update account information
                if (response.data) {
                    // Update Network Information
                    $("#network_id").text(response.data.network_id || '-');
                    $("#network_code").text(response.data.network_code || '-');
                    $("#display_name").text(response.data.display_name || response.data.network_name || '-');
                    
                    // Update Settings
                    $("#timezone").text(response.data.timezone || '-');
                    $("#currency_code").text(response.data.currency_code || '-');
                    
                    // Update Account Details - User Information
                    $("#user_mail").text(response.data.user_mail || '-');
                    $("#user_id").text(response.data.user_id || '-');
                    $("#user_name").text(response.data.user_name || '-');
                    $("#user_role").text(response.data.user_role || '-');
                    
                    // Format user active status
                    var userActiveText = response.data.user_is_active !== undefined ? 
                        (response.data.user_is_active ? 'Yes' : 'No') : '-';
                    $("#user_is_active").text(userActiveText);
                    
                    // Update Account Statistics
                    $("#active_ad_units_count").text(response.data.active_ad_units_count || '0');
                    
                    // Format last updated time
                    var lastUpdated = response.data.last_updated ? 
                        new Date(response.data.last_updated).toLocaleString() : '-';
                    $("#last_updated").text(lastUpdated);
                    
                    // Add additional network information if available
                    var additionalInfo = '';
                    if (response.data.effective_root_ad_unit_id) {
                        additionalInfo += '<p><strong>Root Ad Unit ID:</strong> ' + response.data.effective_root_ad_unit_id + '</p>';
                    }
                    if (response.data.is_test_network !== undefined) {
                        var testNetworkText = response.data.is_test_network ? 'Yes' : 'No';
                        additionalInfo += '<p><strong>Test Network:</strong> ' + testNetworkText + '</p>';
                    }
                    $("#additional_info").html(additionalInfo);
                    
                    // Show note if available
                    if (response.note) {
                        $("#note_text").text(response.note);
                        $("#data_note").show();
                    } else {
                        $("#data_note").hide();
                    }
                    
                    // Show success message
                    showSuccessMessage('Account data loaded successfully!');
                } else {
                    resetAccountDisplay();
                    showErrorMessage('No account data available.');
                }
            } else {
                resetAccountDisplay();
                var errorMsg = response && response.error ? response.error : 'Unknown error occurred';
                showErrorMessage('Error loading account data: ' + errorMsg);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            resetAccountDisplay();
            showErrorMessage('Failed to load account data. Please try again.');
            report_eror(jqXHR, textStatus);
        }
    });
}

function resetAccountDisplay() {
    // Reset all fields to default values
    $("#network_id, #network_code, #display_name, #timezone, #currency_code").text('-');
    $("#user_mail, #user_id, #user_name, #user_role, #user_is_active").text('-');
    $("#active_ad_units_count, #last_updated").text('-');
    $("#additional_info").html('');
    $("#data_note").hide();
}

function showErrorMessage(message) {
    // Create and show a temporary error alert
    var alertHtml = '<div class="alert alert-danger alert-dismissible fade show" role="alert">';
    alertHtml += '<i class="bi bi-exclamation-triangle"></i> ' + message;
    alertHtml += '<button type="button" class="close" data-dismiss="alert" aria-label="Close">';
    alertHtml += '<span aria-hidden="true">&times;</span>';
    alertHtml += '</button>';
    alertHtml += '</div>';
    
    // Insert at the top of the card body
    $('.card-body').first().prepend(alertHtml);
    
    // Auto-hide after 5 seconds
    setTimeout(function() {
        $('.alert-danger').fadeOut('slow', function() {
            $(this).remove();
        });
    }, 5000);
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

function generateRefreshToken() {
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/generate_refresh_token',
        type: 'POST',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function (response) {
            $("#overlay").hide();
            
            if (response && response.status) {
                showSuccessMessage('Refresh token generated successfully!');
                // Reload account data to show updated information
                load_adx_account_data();
            } else {
                var errorMsg = response && response.error ? response.error : 'Unknown error occurred';
                showErrorMessage('Error generating refresh token: ' + errorMsg);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            showErrorMessage('Failed to generate refresh token. Please try again.');
            report_eror(jqXHR, textStatus);
        }
    });
}

function saveOAuthCredentials() {
    var clientId = $('#client_id').val();
    var clientSecret = $('#client_secret').val();
    var networkCode = $('#network_code_input').val();
    var userMail = $('#user_mail_input').val();
    if (!clientId || !clientSecret || !networkCode || !userMail) {
        console.log('Validation failed - missing fields');
        showErrorMessage('Please fill in all OAuth credentials fields.');
        return;
    }
    $("#overlay").show();
    $.ajax({
        url: '/management/admin/save_oauth_credentials',
        type: 'POST',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
        },
        data: {
            'network_code': $('#network_code').val(),
            'client_id': clientId,
            'client_secret': clientSecret,
            'network_code': networkCode,
            'user_mail': userMail
        },
        success: function (response) {
            $("#overlay").hide();
            
            if (response && response.status) {
                showSuccessMessage('OAuth credentials saved successfully!');
                $('#oauthModal').modal('hide');
                // Clear form
                $('#client_id, #client_secret, #network_code, #user_mail').val('');
            } else {
                var errorMsg = response && response.error ? response.error : 'Unknown error occurred';
                showErrorMessage('Error saving OAuth credentials: ' + errorMsg);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            showErrorMessage('Failed to save OAuth credentials. Please try again.');
            report_eror(jqXHR, textStatus);
        }
    });
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