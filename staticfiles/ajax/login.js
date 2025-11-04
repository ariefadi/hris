$(function () {
  'use strict';

  // Login form validation
  $('#loginForm').on('submit', function(e) {
    var username = $('#username').val();
    var password = $('#password').val();
    
    if (!username || !password) {
      e.preventDefault();
      Swal.fire({
        icon: 'error',
        title: 'Error',
        text: 'Please fill in all fields'
      });
      return false;
    }
  });

  // Google OAuth login
  $('.btn-google').on('click', function(e) {
    e.preventDefault();
    window.location.href = '/accounts/login/google-oauth2/';
  });

  // Show/hide password
  $('.show-password').on('click', function() {
    var passwordField = $('#password');
    var type = passwordField.attr('type') === 'password' ? 'text' : 'password';
    passwordField.attr('type', type);
    $(this).find('i').toggleClass('fa-eye fa-eye-slash');
  });
});