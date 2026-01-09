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

    $('#account_filter').select2({
        placeholder: '-- Pilih Akun Terdaftar --',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4'
    });

    $('#btn_load_data').click(function (e) {
        load_adx_account_data();
    });
});
$('#assignAccountModal').on('shown.bs.modal', function () {
    $('#user_akun').select2({
        placeholder: '-- Pilih Akun User --',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        dropdownParent: $('#assignAccountModal')
    });
});
function load_adx_account_data() {
    $("#overlay").show();
    var selectedAccounts = $('#account_filter').val();
    $.ajax({
        url: '/management/admin/page_adx_user_account',
        type: 'GET',
        data: {
            'selected_accounts': selectedAccounts
        },
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

// Handle Edit Account Name Modal
$(document).ready(function() {
    console.log('Account.js loaded - Modal handler initialized');
    
    // Function to get cookie value
    function getCookie(name) {
        var cookieValue = null;
        if (document.cookie && document.cookie !== '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = cookies[i].trim();
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    }

    // Handle Assign Account Name buttons
    $(document).on('click', '.assign-account-data', function() {
        console.log('Assign button clicked');
        var account_adx = $(this).data('account-id');
        // Set the hidden input value
        $('#assign_account_adx').val(account_adx);
        // Show the modal (Bootstrap 4 syntax)
        $('#assignAccountModal').modal('show');
    });
    
    $(document).on('click', '.delete-account-credentials', function() {
        var userMail = $(this).data('email');
        var accountName = $(this).data('account-name');

        var label = accountName ? (accountName + ' (' + userMail + ')') : userMail;
        var confirmed = confirm('Yakin hapus kredensial: ' + label + ' ?');
        if (!confirmed) {
            return;
        }

        $.ajax({
            url: '/management/admin/delete_adx_account_credentials',
            type: 'POST',
            data: {
                'user_mail': userMail,
                'csrfmiddlewaretoken': getCookie('csrftoken')
            },
            success: function(response) {
                if (response && response.status) {
                    showAlert('success', response.message || 'Kredensial berhasil dihapus');
                    location.reload();
                } else {
                    showAlert('error', (response && response.message) ? response.message : 'Gagal menghapus kredensial');
                }
            },
            error: function(xhr) {
                var errorMessage = 'Terjadi kesalahan saat menghapus';
                if (xhr && xhr.responseJSON && xhr.responseJSON.message) {
                    errorMessage = xhr.responseJSON.message;
                } else if (xhr && xhr.status === 403) {
                    errorMessage = 'Unauthorized';
                }
                window.showAlert('error', errorMessage);
            }
        });
    });

    // Handle Edit Account Name buttons
    $(document).on('click', '.edit-account-name', function() {
        console.log('Edit button clicked');
        var email = $(this).data('email');
        var accountName = $(this).data('account-name');
        console.log('Data:', email, accountName);
        // Populate modal fields
        $('#edit_user_mail').val(email);
        $('#edit_account_name').val(accountName);
        // Show the modal (Bootstrap 4 syntax)
        $('#editAccountNameModal').modal('show');
        console.log('Modal show called');
    });
    
    // Alert function
    function showAlert(message, type) {
        var alertClass = type === 'success' ? 'alert-success' : 'alert-danger';
        var alertHtml = '<div class="alert ' + alertClass + ' alert-dismissible fade show" role="alert">' +
                       message +
                       '<button type="button" class="close" data-dismiss="alert" aria-label="Close">' +
                       '<span aria-hidden="true">&times;</span>' +
                       '</button>' +
                       '</div>';
        
        // Remove existing alerts
        $('.alert').remove();
        
        // Add new alert at the top of the modal body
        $('.modal-body').prepend(alertHtml);
        
        // Auto-hide after 5 seconds
        setTimeout(function() {
            $('.alert').fadeOut();
        }, 5000);
    }

    // Handle Save Assign Account button
    $('#btn_save_assign_account').click(function() {
        var account_adx = $('#assign_account_adx').val();
        console.log('Account ADX:', account_adx);
        var userAkun = $('#user_akun').val();
        if (!userAkun) {
            showAlert('User akun tidak boleh kosong!', 'error');
            return;
        }
        // Show loading state
        $(this).prop('disabled', true).html('<i class="fas fa-spinner fa-spin"></i> Saving...');
        // AJAX request to update account name
        $.ajax({
            url: '/management/admin/assign_account_user',
            type: 'POST',
            data: {
                'account_id': account_adx,
                'user_akun[]': userAkun,
                'csrfmiddlewaretoken': getCookie('csrftoken')
            },
            success: function(response) {
                console.log('AJAX Success Response:', response);
                $('#btn_save_assign_account').prop('disabled', false).text('Save Changes');
                
                if (response.status) {
                    window.showAlert('success', response.message);
                    $('#assignAccountModal').modal('hide');
                    // Refresh the page to show updated data
                    location.reload();
                } else {
                    window.showAlert('error', response.message || 'Gagal assign account user');
                }
            },
            error: function(xhr, status, error) {
                console.error('AJAX Error:', {
                    status: xhr.status,
                    statusText: xhr.statusText,
                    responseText: xhr.responseText,
                    error: error
                });
                $('#btn_save_account_name').prop('disabled', false).text('Save Changes');
                
                let errorMessage = 'Terjadi kesalahan saat menyimpan';
                if (xhr.responseJSON && xhr.responseJSON.message) {
                    errorMessage = xhr.responseJSON.message;
                } else if (xhr.status === 403) {
                    errorMessage = 'CSRF verification failed. Silakan refresh halaman dan coba lagi.';
                } else if (xhr.status === 500) {
                    errorMessage = 'Server error. Silakan coba lagi.';
                }
                
                showAlert('error', errorMessage);
            },
            complete: function() {
                // Reset button state
                $('#btn_save_account_name').prop('disabled', false).html('<i class="bi bi-check"></i> Save Changes');
            }
        });
    });

    // Handle Save Account Name button
    $('#btn_save_account_name').click(function() {
        var userMail = $('#edit_user_mail').val();
        var newAccountName = $('#edit_account_name').val().trim();
        
        if (!newAccountName) {
            alert('Account name tidak boleh kosong!');
            return;
        }
        
        // Show loading state
        $(this).prop('disabled', true).html('<i class="fas fa-spinner fa-spin"></i> Saving...');
        
        // AJAX request to update account name
        $.ajax({
            url: '/management/admin/update_account_name',
            type: 'POST',
            data: {
                'user_mail': userMail,
                'account_name': newAccountName,
                'csrfmiddlewaretoken': getCookie('csrftoken')
            },
            success: function(response) {
                console.log('AJAX Success Response:', response);
                $('#btn_save_account_name').prop('disabled', false).text('Save Changes');
                
                if (response.status) {
                    window.showAlert('success', response.message);
                    $('#editAccountNameModal').modal('hide');
                    // Refresh the page to show updated data
                    location.reload();
                } else {
                    window.showAlert('error', response.message || 'Gagal mengupdate account name');
                }
            },
            error: function(xhr, status, error) {
                console.error('AJAX Error:', {
                    status: xhr.status,
                    statusText: xhr.statusText,
                    responseText: xhr.responseText,
                    error: error
                });
                $('#btn_save_account_name').prop('disabled', false).text('Save Changes');
                
                let errorMessage = 'Terjadi kesalahan saat menyimpan';
                if (xhr.responseJSON && xhr.responseJSON.message) {
                    errorMessage = xhr.responseJSON.message;
                } else if (xhr.status === 403) {
                    errorMessage = 'CSRF verification failed. Silakan refresh halaman dan coba lagi.';
                } else if (xhr.status === 500) {
                    errorMessage = 'Server error. Silakan coba lagi.';
                }
                
                showAlert('error', errorMessage);
            },
            complete: function() {
                // Reset button state
                $('#btn_save_account_name').prop('disabled', false).html('<i class="bi bi-check"></i> Save Changes');
            }
        });
    });
});

function showAlert(type, message) {
    var alertClass = type === 'success' ? 'alert-success' : 'alert-danger';
    var iconClass = type === 'success' ? 'fas fa-check-circle' : 'fas fa-exclamation-triangle';
    
    var alertHtml = '<div class="alert ' + alertClass + ' alert-dismissible fade show" role="alert">' +
        '<i class="' + iconClass + '"></i> ' + message +
        '<button type="button" class="close" data-dismiss="alert" aria-label="Close">' +
        '<span aria-hidden="true">&times;</span>' +
        '</button>' +
        '</div>';
    
    // Remove existing alerts
    $('.alert-success, .alert-danger').remove();
    
    // Add new alert at the top of the card body
    $('.card-body').first().prepend(alertHtml);
    
    // Auto-hide after 5 seconds
    setTimeout(function() {
        $('.alert').fadeOut();
    }, 5000);
}