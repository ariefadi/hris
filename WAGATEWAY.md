## Login
- Method: GET
- Path: /app/login
- Response: {"code":"SUCCESS","message":"Login success","results":{"qr_duration":30,"qr_link":"http://localhost:3000/statics/qrcode/scan-qr-15b6a564-a90f-47f0-aaf2-9a19319940f2.png"}}
- The response if alredy loged in: {"code":"SUCCESS","message":"Already logged in","results":{}}

## Logout
- Method: GET
- Path: /app/logout
- Response: {"code":"SUCCESS","message":"Success logout"}

## Reconnect
- Method: GET
- Path: /app/reconnect
- Response: {"code":"SUCCESS","message":"Reconnect success"}

## Devices
- Method: GET
- Path: /app/devices
- Response: {"code":"SUCCESS","message":"Fetch device success","results":[{"name":"Hendrik","device":"62895xxxxxxx:15@s.whatsapp.net"}]}

## Send Message
- Method: POST
- Path: /send/message
- Request Body:
  ```json
  {"phone":"6282198913987@s.whatsapp.net","message":"Juahahaha","is_forwarded":false}
  ```
- Response: {"code":"SUCCESS","message":"Message sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:24:34 +0000 UTC)","results":{"message_id":"3EB0BB727804AF848FE142","status":"Message sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:24:34 +0000 UTC)"}}

## Send Image
- Method: POST
- Path: /send/image
- Request Payload:
    ```
    Content-Disposition: form-data; name="phone"
    6282198913987@s.whatsapp.net

    Content-Disposition: form-data; name="view_once"
    false

    Content-Disposition: form-data; name="compress"
    false

    Content-Disposition: form-data; name="caption"
    Ini gambar

    Content-Disposition: form-data; name="is_forwarded"
    false

    Content-Disposition: form-data; name="image"; filename="avatashika.png"
    Content-Type: image/png
    ```
- Response: {"code":"SUCCESS","message":"Message sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:24:34 +0000 UTC)","results":{"message_id":"3EB0BB727804AF848FE142","status":"Message sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:24:34 +0000 UTC)"}}

## Send File
- Method: POST
- Path: /send/file
- Request Payload:
    ```
    Content-Disposition: form-data; name="phone"
    62821xxxxxx@s.whatsapp.net

    Content-Disposition: form-data; name="caption"
    Ini file PDF

    Content-Disposition: form-data; name="is_forwarded"
    false

    Content-Disposition: form-data; name="file"; filename="document.pdf"
    Content-Type: application/pdf
    ```
- Response: {"code":"SUCCESS","message":"Document sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:28:07 +0000 UTC)","results":{"message_id":"3EB04DB6733398A7F7B9A6","status":"Document sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:28:07 +0000 UTC)"}}

## Send Link
- Method: POST
- Path: /send/link
- Request JSON:
  ```json
  {"phone":"6282198913987@s.whatsapp.net","link":"https://facebook.com","caption":"Ini link facebook","is_forwarded":false}
  ```
- Response: {"code":"SUCCESS","message":"Link sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:30:08 +0000 UTC)","results":{"message_id":"3EB0BECDB79E29FB69D321","status":"Link sent to 62821xxxxxx@s.whatsapp.net (server timestamp: 2025-12-20 02:30:08 +0000 UTC)"}}
