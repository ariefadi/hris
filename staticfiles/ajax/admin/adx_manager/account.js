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
    
    $('#btn_generate_refresh_token').click(function (e) {
        e.preventDefault();
        // Show the integrated OAuth modal instead of calling the old function
        showOAuthRefreshTokenModal();
    });
    
    $('#btn_oauth_setup').click(function (e) {
        e.preventDefault();
        
        // Auto-fill user email from current user data
        var currentUserEmail = $('#user_email').text();
        if (currentUserEmail && currentUserEmail !== '-') {
            $('#oauth_user_email').val(currentUserEmail);
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
    $.ajax({
        url: '/management/admin/page_adx_user_account',
        type: 'GET',
        success: function(response) {
            if (response.status) {
                // Update network info - sesuaikan dengan ID di template HTML
                $("#network_id").text(response.data.network_id || '-');
                $("#network_code").text(response.data.network_code || '-');
                $("#display_name").text(response.data.display_name || response.data.network_name || 'AdX Network');

                // Update settings - sesuaikan dengan ID di template HTML
                $("#timezone").text(response.data.timezone || 'Asia/Jakarta');
                $("#currency_code").text(response.data.currency_code || 'USD');

                // Update user info - sesuaikan dengan ID di template HTML
                $("#user_mail").text(response.data.user_mail || '-');
                $("#user_id").text(response.data.user_id || '-');
                $("#user_name").text(response.data.user_name || '-');
                $("#user_role").text(response.data.user_role || '-');
                $("#user_is_active").text(response.data.user_is_active === true ? 'Yes' : (response.data.user_is_active === false ? 'No' : response.data.user_is_active || 'Yes'));

                // Update account stats - sesuaikan dengan ID di template HTML
                $("#active_ad_units_count").text(response.data.active_ad_units_count || '0');
                $("#last_updated").text(response.data.last_updated || '-');

                showSuccessMessage("Data berhasil dimuat");
            } else {
                showErrorMessage(response.message || "Gagal memuat data");
            }
        },
        error: function(xhr, status, error) {
            showErrorMessage("Error: " + error);
        }
    });
}

function resetAccountDisplay() {
    // Reset network info - sesuaikan dengan ID di template HTML
    $("#network_id, #network_code, #display_name").text('-');
    
    // Reset settings - sesuaikan dengan ID di template HTML
    $("#timezone, #currency_code").text('-');
    
    // Reset user info - sesuaikan dengan ID di template HTML
    $("#user_mail, #user_id, #user_name, #user_role, #user_is_active").text('-');
    
    // Reset account stats - sesuaikan dengan ID di template HTML
    $("#active_ad_units_count").text('-');
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
            'X-CSRFToken': csrftoken
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
    var userMail = $('#oauth_user_mail').val();

    // Validasi input
    if (!clientId || !clientSecret || !userMail) {
        showErrorMessage("Semua field harus diisi");
        return;
    }

    // Kirim data ke server
    $.ajax({
        url: '/management/admin/save_oauth_credentials',
        type: 'POST',
        data: {
            'client_id': clientId,
            'client_secret': clientSecret,
            'user_mail': userMail
        },
        success: function(response) {
            if (response.status) {
                showSuccessMessage("Kredensial OAuth berhasil disimpan");
                $('#oauthModal').modal('hide');
                $('#client_id, #client_secret, #oauth_user_mail').val('');
                load_adx_account_data();
            } else {
                showErrorMessage(response.message || "Gagal menyimpan kredensial");
            }
        },
        error: function(xhr, status, error) {
            showErrorMessage("Error: " + error);
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

const csrftoken = getCookie('csrftoken');

// OAuth Refresh Token Generation Functions
function showOAuthRefreshTokenModal() {
    $('#oauthRefreshTokenModal').modal('show');
    resetOAuthModal();
}

function resetOAuthModal() {
    $('#oauth-step-1').show();
    $('#oauth-step-2').hide();
    $('#oauth-email').val('');
    $('#oauth-auth-code').val('');
    $('#oauth-url-display').empty();
    $('#oauth-messages').empty();
}

function generateOAuthURL() {
    const email = $('#oauth-email').val().trim();
    
    if (!email) {
        showOAuthMessage('Please enter an email address', 'error');
        return;
    }
    
    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
        showOAuthMessage('Please enter a valid email address', 'error');
        return;
    }
    
    showOAuthMessage('Generating OAuth URL...', 'info');
    
    $.ajax({
        url: '/management/admin/generate_oauth_url',
        type: 'POST',
        data: {
            'email': email,
            'csrfmiddlewaretoken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function(response) {
            if (response.success) {
                $('#oauth-url-display').html(`
                    <div class="alert alert-info">
                        <strong>Step 1 Complete!</strong><br>
                        Click the link below to authorize with Google:<br>
                        <a href="${response.oauth_url}" target="_blank" class="btn btn-primary btn-sm mt-2">
                            <i class="fas fa-external-link-alt"></i> Authorize with Google
                        </a>
                    </div>
                `);
                $('#oauth-step-1').hide();
                $('#oauth-step-2').show();
                showOAuthMessage('OAuth URL generated successfully. Please click the link above to authorize.', 'success');
            } else {
                showOAuthMessage(response.message || 'Failed to generate OAuth URL', 'error');
            }
        },
        error: function(xhr, status, error) {
            showOAuthMessage('Error generating OAuth URL: ' + error, 'error');
        }
    });
}

function submitAuthCode() {
    const email = $('#oauth-email').val().trim();
    const authCode = $('#oauth-auth-code').val().trim();
    
    if (!authCode) {
        showOAuthMessage('Please enter the authorization code', 'error');
        return;
    }
    
    showOAuthMessage('Processing authorization code...', 'info');
    
    $.ajax({
        url: '/management/admin/process_oauth_code',
        type: 'POST',
        data: {
            'email': email,
            'auth_code': authCode,
            'csrfmiddlewaretoken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function(response) {
            if (response.success) {
                showOAuthMessage('Refresh token generated successfully!', 'success');
                setTimeout(function() {
                    $('#oauthRefreshTokenModal').modal('hide');
                    loadAccountData(); // Reload account data
                }, 2000);
            } else {
                showOAuthMessage(response.message || 'Failed to process authorization code', 'error');
            }
        },
        error: function(xhr, status, error) {
            showOAuthMessage('Error processing authorization code: ' + error, 'error');
        }
    });
}

function showOAuthMessage(message, type) {
    const alertClass = type === 'error' ? 'alert-danger' : 
                      type === 'success' ? 'alert-success' : 'alert-info';
    
    $('#oauth-messages').html(`
        <div class="alert ${alertClass} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="close" data-dismiss="alert">
                <span>&times;</span>
            </button>
        </div>
    `);
}