from django.views import View
from django.shortcuts import render, redirect
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.contrib import messages
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.clickjacking import xframe_options_sameorigin
from projects.database import data_mysql
from datetime import datetime, timedelta
import calendar
from hris.mail import send_mail, Mail
from hris.wa import send_whatsapp_message
from management import views as management_views
import re
import random
import requests
import json
import struct
import zlib
import html
import os
import tempfile
from urllib.parse import urlparse
from django.contrib.staticfiles import finders
from django.templatetags.static import static
from io import BytesIO

def _escape_pdf_text(value):
    s = str(value or '')
    s = s.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
    return s

def _format_pdf_number(value):
    try:
        num = float(value)
    except Exception:
        num = 0
    n = int(round(num))
    s = f"{n:,}"
    return s.replace(',', '.')

def _build_invoice_pdf_html(title, account_name, month_label, header_cells, row_cells, total_text, request=None):
    from xhtml2pdf import pisa
    
    safe_title = html.escape(str(title or ''))
    safe_account = html.escape(str(account_name or '-'))
    safe_month = html.escape(str(month_label or '-'))
    safe_total = html.escape(str(total_text or ''))
    header_colors = ["#F5F5F5", "#81C784", "#EF5350", "#EF5350", "#E0E0E0", "#64B5F6", "#66BB6A"]
    header_html = "".join(
        (
            f"<th style='background:{header_colors[i]};'>"
            f"{html.escape(str(cell or ''))}"
            f"<br><small>Server, admin fee &amp; tax</small>"
            f"</th>"
            if str(cell or '') == "Total Expenses"
            else f"<th style='background:{header_colors[i]};'>{html.escape(str(cell or ''))}</th>"
        )
        for i, cell in enumerate(header_cells)
    )
    row_html = "".join(
        f"<td>{html.escape(str(cell or ''))}</td>"
        for cell in row_cells
    )
    logo_path = request.build_absolute_uri(static("dist/assets/img/adsense-logo.jpg"))
    html_doc = f"""
    <html>
      <head>
        <meta charset="utf-8" />
        <style>
          @page {{
            
            size: 297.04mm 209.9mm;
            margin: 10mm;
          }}
          body {{
            font-family: Roboto, Arial, sans-serif;
            font-size: 11pt;
            color: #111;
          }}
          h1 {{
            text-align: center;
            font-size: 14pt;
            margin: 0 0 8mm 0;
          }}
          .meta {{
            margin-bottom: 6mm;
          }}
          .meta div {{
            margin-bottom: 2mm;
          }}
          table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 8mm;
            font-size: 10pt;
          }}
          th, td {{
            border: 1px solid #d0d0d0;
            padding: 6px 8px;
          }}
          th {{
            text-align: center;
            font-weight: bold;
          }}
          td:nth-child(n+2) {{
            text-align: right;
          }}
          .total-line {{
            margin-top: 6mm;
          }}
          .total-amount {{
            color: #2E7D32;
            font-weight: bold;
          }}
          .footer {{
            margin-top: 3mm;
          }}
          .logo {{
            margin-top: 6mm;
            width: 100%;
            text-align: center;
          }}
          .logo img {{
            width: 240px;
          }}
        </style>
      </head>
      <body>
        <h1>INVOICE ADSENSE - SHARING PROFIT</h1>
        <div class="meta">
            <div>
                Adsense Account: {safe_account}
                <br>
                Month: {safe_month}
            </div>
        </div>
        <table>
          <thead>
            <tr>{header_html}</tr>
          </thead>
          <tbody>
            <tr>{row_html}</tr>
          </tbody>
        </table>
        <div class="total-line">
          Total yang akan di transfer adalah <span class="total-amount">{safe_total}</span>
          <br>
          Jika ingin penjelasan lebih lanjut, silahkan hubungi melalui WhatsApp.
        </div>
        <div class="logo"><img src="{logo_path}" alt="logo" /></div>
      </body>
    </html>
    """
    output = BytesIO()
    result = pisa.CreatePDF(html_doc, dest=output)
    if result.err:
        return None
    return output.getvalue()

def _read_png_rgb(path):
    try:
        with open(path, 'rb') as f:
            data = f.read()
    except Exception:
        return None
    if data[:8] != b'\x89PNG\r\n\x1a\n':
        return None
    pos = 8
    width = height = bit_depth = color_type = None
    compression = filter_method = interlace = None
    idat = b''
    while pos + 8 <= len(data):
        length = struct.unpack('>I', data[pos:pos + 4])[0]
        pos += 4
        ctype = data[pos:pos + 4]
        pos += 4
        chunk = data[pos:pos + length]
        pos += length
        pos += 4
        if ctype == b'IHDR':
            width, height, bit_depth, color_type, compression, filter_method, interlace = struct.unpack('>IIBBBBB', chunk)
        elif ctype == b'IDAT':
            idat += chunk
        elif ctype == b'IEND':
            break
    if not idat or width is None or height is None:
        return None
    if bit_depth != 8 or compression != 0 or filter_method != 0 or interlace != 0:
        return None
    if color_type not in (2, 6):
        return None
    try:
        raw = zlib.decompress(idat)
    except Exception:
        return None
    bpp = 3 if color_type == 2 else 4
    stride = width * bpp
    out = bytearray()
    prev = bytearray(stride)
    offset = 0
    def paeth(a, b, c):
        p = a + b - c
        pa = abs(p - a)
        pb = abs(p - b)
        pc = abs(p - c)
        if pa <= pb and pa <= pc:
            return a
        if pb <= pc:
            return b
        return c
    for _ in range(height):
        if offset >= len(raw):
            return None
        filter_type = raw[offset]
        offset += 1
        scan = raw[offset:offset + stride]
        offset += stride
        row = bytearray(stride)
        for i in range(stride):
            x = scan[i]
            if filter_type == 0:
                row[i] = x
            elif filter_type == 1:
                left = row[i - bpp] if i >= bpp else 0
                row[i] = (x + left) & 0xFF
            elif filter_type == 2:
                up = prev[i]
                row[i] = (x + up) & 0xFF
            elif filter_type == 3:
                left = row[i - bpp] if i >= bpp else 0
                up = prev[i]
                row[i] = (x + ((left + up) // 2)) & 0xFF
            elif filter_type == 4:
                left = row[i - bpp] if i >= bpp else 0
                up = prev[i]
                up_left = prev[i - bpp] if i >= bpp else 0
                row[i] = (x + paeth(left, up, up_left)) & 0xFF
            else:
                return None
        if color_type == 6:
            for i in range(0, len(row), 4):
                out.extend(row[i:i + 3])
        else:
            out.extend(row)
        prev = row
    return width, height, bytes(out)

def _build_pdf(content_text, page_width=612, page_height=792, image_data=None):
    content_bytes = content_text.encode("latin-1", "replace")
    pdf = bytearray()
    def write(text):
        pdf.extend(text.encode("latin-1"))
    write("%PDF-1.4\n")
    offsets = []
    def write_obj(obj_num, body):
        offsets.append(len(pdf))
        write(f"{obj_num} 0 obj\n{body}\nendobj\n")
    write_obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
    write_obj(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    xobject = " /XObject << /Im1 6 0 R >>" if image_data else ""
    write_obj(3, f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {page_width} {page_height}] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >>{xobject} >> >>")
    stream_body = f"<< /Length {len(content_bytes)} >>\nstream\n{content_text}\nendstream"
    write_obj(4, stream_body)
    write_obj(5, "<< /Type /Font /Subtype /Type1 /BaseFont /Roboto /Encoding /WinAnsiEncoding >>")
    if image_data:
        offsets.append(len(pdf))
        write(f"6 0 obj\n")
        header = f"<< /Type /XObject /Subtype /Image /Width {image_data['width']} /Height {image_data['height']} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /FlateDecode /Length {len(image_data['data'])} >>"
        write(f"{header}\nstream\n")
        pdf.extend(image_data['data'])
        write("\nendstream\nendobj\n")
    xref_offset = len(pdf)
    write("xref\n")
    write(f"0 {len(offsets) + 1}\n")
    write("0000000000 65535 f \n")
    for off in offsets:
        write(f"{off:010d} 00000 n \n")
    write("trailer\n")
    write(f"<< /Size {len(offsets) + 1} /Root 1 0 R >>\n")
    write("startxref\n")
    write(f"{xref_offset}\n%%EOF\n")
    return bytes(pdf)

def _build_simple_pdf(title, lines):
    content_lines = []
    content_lines.append("BT")
    content_lines.append("/F1 12 Tf")
    content_lines.append("72 760 Td")
    content_lines.append(f"({_escape_pdf_text(title)}) Tj")
    content_lines.append("0 -18 Td")
    content_lines.append("/F1 9 Tf")
    for line in lines:
        content_lines.append(f"({_escape_pdf_text(line)}) Tj")
        content_lines.append("0 -14 Td")
    content_lines.append("ET")
    content_text = "\n".join(content_lines)
    return _build_pdf(content_text)

def _build_invoice_pdf(title, account_name, month_label, header_cells, row_cells, total_text):
    def _hex_to_rgb(hex_color):
        s = str(hex_color or '').lstrip('#')
        if len(s) != 6:
            return (0, 0, 0)
        try:
            r = int(s[0:2], 16) / 255
            g = int(s[2:4], 16) / 255
            b = int(s[4:6], 16) / 255
            return (r, g, b)
        except Exception:
            return (0, 0, 0)
    def add_rect(x, y, w, h, hex_color):
        r, g, b = _hex_to_rgb(hex_color)
        content_lines.append("q")
        content_lines.append(f"{r:.3f} {g:.3f} {b:.3f} rg")
        content_lines.append(f"{x} {y} {w} {h} re")
        content_lines.append("f")
        content_lines.append("Q")
    def add_line(text, x, y, size):
        content_lines.append(f"/F1 {size} Tf")
        content_lines.append(f"1 0 0 1 {x} {y} Tm")
        content_lines.append(f"({_escape_pdf_text(text)}) Tj")
    def add_line_color(text, x, y, size, hex_color, bold=False):
        r, g, b = _hex_to_rgb(hex_color)
        content_lines.append(f"{r:.3f} {g:.3f} {b:.3f} rg")
        content_lines.append(f"/F1 {size} Tf")
        content_lines.append(f"1 0 0 1 {x} {y} Tm")
        content_lines.append(f"({_escape_pdf_text(text)}) Tj")
        if bold:
            content_lines.append(f"1 0 0 1 {x + 0.5} {y} Tm")
            content_lines.append(f"({_escape_pdf_text(text)}) Tj")
        content_lines.append("0 0 0 rg")
    content_lines = []
    page_width = 842
    page_height = 595
    heading = "INVOICE ADSENSE - SHARING PROFIT"
    heading_size = 14
    heading_x = max(36, int((page_width - (len(heading) * heading_size * 0.6)) / 2))
    left_margin = 42
    right_margin = 42
    header_x = [42, 220, 320, 420, 520, 630, 730]  # X start for each header column
    row_x = header_x  # X start for each data column
    header_colors = ["#F5F5F5", "#81C784", "#EF5350", "#EF5350", "#E0E0E0", "#64B5F6", "#66BB6A"]  # Header background colors per column
    header_top = 490  # Top Y of header band
    header_height = 22  # Header band height (vertical padding)
    header_text_y = header_top - 14  # Header text baseline inside the band
    y_row = header_top - header_height - 15  # Data row baseline under header
    header_widths = []  # Width for each header column
    for i in range(len(header_x) - 1):
        header_widths.append(header_x[i + 1] - header_x[i])  # Width from this column start to next
    header_widths.append(page_width - right_margin - header_x[-1])  # Width from last column to right margin
    for i in range(len(header_cells)):
        add_rect(header_x[i], header_top - header_height, header_widths[i], header_height, header_colors[i])  # Draw header background
    logo_path = r"C:\Users\Hendrik\Documents\GitHub\hris\staticfiles\admin\img\adsense-logo.png"
    image_data = None
    image_cmd = None
    logo = _read_png_rgb(logo_path)
    if logo:
        img_w, img_h, img_rgb = logo
        max_w = 140
        max_h = 40
        scale = min(max_w / img_w, max_h / img_h, 1)
        draw_w = img_w * scale
        draw_h = img_h * scale
        img_x = (page_width - draw_w) / 2
        img_y = 20
        image_data = {
            'width': img_w,
            'height': img_h,
            'data': zlib.compress(img_rgb)
        }
        image_cmd = f"q {draw_w:.2f} 0 0 {draw_h:.2f} {img_x:.2f} {img_y:.2f} cm /Im1 Do Q"
    content_lines.append("BT")
    add_line(heading, heading_x, 560, heading_size)
    add_line(f"Adsense Account: {account_name}", 42, 530, 10)
    add_line(f"Month: {month_label}", 42, 515, 10)
    header_font_size = 9
    for i, cell in enumerate(header_cells):
        cell_width = header_widths[i]
        text_width = len(str(cell)) * header_font_size * 0.6
        text_x = header_x[i] + max(0, (cell_width - text_width) / 2)
        add_line(cell, text_x, header_text_y, header_font_size)
    for i, cell in enumerate(row_cells):
        add_line(cell, row_x[i], y_row, 9)
    total_prefix = "Total yang akan di transfer adalah "
    total_prefix_x = 42
    total_prefix_y = 400
    total_prefix_size = 10
    add_line(total_prefix, total_prefix_x, total_prefix_y, total_prefix_size)
    total_prefix_width = len(total_prefix) * total_prefix_size * 0.6
    add_line_color(total_text, total_prefix_x + total_prefix_width, total_prefix_y, total_prefix_size, "#2E7D32", True)
    add_line("Jika ingin penjelasan lebih lanjut, silahkan hubungi melalui WhatsApp.", 42, 385, 10)
    content_lines.append("ET")
    if image_cmd:
        content_lines.append(image_cmd)
    content_text = "\n".join(content_lines)
    return _build_pdf(content_text, page_width=page_width, page_height=page_height, image_data=image_data)


def send_mail_notification(to, subject, body=None):
    try:
        if not to:
            return False
        return bool(send_mail(to=to, subject=subject, template='emails/simple.html', body=body, context={'body': body, 'subject': subject, 'brand_name': 'Trend Horizone'}))
    except Exception:
        return False

def send_whatsapp_notification(recipients, message):
    try:
        if not recipients:
            return False
        emails = [str(v or '').strip().lower() for v in recipients if v]
        emails = list(dict.fromkeys(emails))
        if not emails:
            return False
        if not normalized_periode:
            now = datetime.now()
            normalized_periode = now.strftime('%Y-%m')
        db = data_mysql()
        placeholders = ','.join(['%s'] * len(emails))
        sql = f"SELECT user_mail, user_telp FROM app_users WHERE LOWER(user_mail) IN ({placeholders})"
        if not db.execute_query(sql, tuple(emails)):
            return False
        rows = db.cur_hris.fetchall() or []
        phones = []
        for r in rows:
            try:
                telp = r.get('user_telp')
            except AttributeError:
                try:
                    telp = r[1]
                except Exception:
                    telp = None
            s = str(telp or '').strip()
            if not s:
                continue
            s = re.sub(r'\D+', '', s)
            if not s:
                continue
            if s.startswith('0'):
                s = '62' + s[1:]
            elif s.startswith('62'):
                s = s
            elif s.startswith('8'):
                s = '62' + s
            elif s.startswith('620'):
                s = '62' + s[3:]
            phones.append(s)
        phones = list(dict.fromkeys([p for p in phones if p]))
        if not phones:
            return False
        msg = str(message or '')
        msg = re.sub(r'(?i)<br\s*/?>', '\n', msg)
        msg = re.sub(r'(?i)</p\s*>', '\n\n', msg)
        msg = re.sub(r'(?i)<p\s*>', '', msg)
        msg = re.sub(r'<[^>]+>', '', msg)
        msg = re.sub(r'\n{3,}', '\n\n', msg)
        msg = msg.strip()
        if not msg:
            return False
        return bool(send_whatsapp_message(to=phones, message=msg, is_forwarded=False, delay_min_ms=1000, delay_max_ms=5000))
    except Exception:
        return False

class DraftIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT *
            FROM data_media_partner
            WHERE status = 'draft'
            ORDER BY COALESCE(mdd, request_date) DESC
        """
        partners = []
        if db.execute_query(sql):
            partners = db.cur_hris.fetchall() or []
        try:
            pids = []
            for r in partners:
                try:
                    pid = r.get('partner_id')
                except AttributeError:
                    pid = None
                if pid:
                    pids.append(str(pid))
            dom_map = {}
            if pids:
                placeholders = ",".join(["%s"] * len(pids))
                q_domrels = f"""
                    SELECT pd.partner_id, d.domain
                    FROM data_media_partner_domain pd
                    JOIN data_domains d ON d.domain_id = pd.domain_id
                    WHERE pd.partner_id IN ({placeholders})
                    ORDER BY d.domain ASC
                """
                if db.execute_query(q_domrels, tuple(pids)):
                    for rr in (db.cur_hris.fetchall() or []):
                        try:
                            pid = rr.get('partner_id')
                            dom = rr.get('domain')
                        except AttributeError:
                            pid = rr[0]
                            dom = rr[1]
                        if not pid:
                            continue
                        lst = dom_map.get(pid)
                        if not lst:
                            lst = []
                            dom_map[pid] = lst
                        if dom:
                            lst.append(str(dom))
            for r in partners:
                try:
                    pid = r.get('partner_id')
                    r['domains'] = dom_map.get(pid) or []
                    r['domain_count'] = len(r['domains'])
                except AttributeError:
                    pass
        except Exception:
            pass

        users = []
        q_users = """
            SELECT user_alias
            FROM app_users
            ORDER BY user_alias ASC
        """
        if db.execute_query(q_users):
            users = db.cur_hris.fetchall() or []
            
        domains = []
        q_domains = """
            SELECT domain_id, domain AS domain_label
            FROM data_domains
            ORDER BY domain ASC
        """
        if db.execute_query(q_domains):
            domains = db.cur_hris.fetchall() or []

        statuses = ['draft','waiting','canceled','rejected','completed']
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partners': partners,
            'users': users,
            'domains': domains,
            'statuses': statuses,
        }
        return render(request, 'task/draft/index.html', context)

class DraftCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        partner_id = f"MP{base}{rnd}"[:20]
        partner_name = request.POST.get('partner_name', '').strip()
        partner_contact = request.POST.get('partner_contact', '').strip()
        partner_region = request.POST.get('partner_region', '').strip()
        request_date_raw = request.POST.get('request_date', '').strip()
        pic = request.POST.get('pic', '').strip()
        adnetwork = request.POST.get('adnetwork', '').strip() or 'adx'
        domain_ids = request.POST.getlist('domains')
        status = 'draft'
        try:
            fixed = request_date_raw.strip()
            request_date = datetime.strptime(fixed, '%Y-%m-%d') if fixed else None
        except ValueError:
            request_date = None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not partner_name:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Nama partner wajib diisi.'}, status=400)
            messages.error(request, 'Nama partner wajib diisi.')
            return redirect('/projects/task/draft')
        # if not domain_ids:
        #     messages.error(request, 'Domain wajib dipilih.')
        #     return redirect('/projects/task/draft')
        sql = """
            INSERT INTO data_media_partner
            (partner_id, partner_name, partner_contact, partner_region, request_date, pic, status, adnetwork, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params = (
            partner_id,
            partner_name,
            partner_contact or None,
            partner_region or None,
            request_date,
            pic or None,
            status,
            adnetwork,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
        )

        if db.execute_query(sql, params):
            db.commit()
            sql_rel = """
                INSERT INTO data_media_partner_domain (partner_id, domain_id)
                VALUES (%s, %s)
            """
            for did in domain_ids:
                val = (str(did or '').strip())
                if not val:
                    continue
                try:
                    num = int(val)
                except ValueError:
                    continue
                db.execute_query(sql_rel, (partner_id, num))
            db.commit()
            if is_ajax:
                return JsonResponse({'status': True, 'partner_id': partner_id})
            messages.success(request, 'Project berhasil ditambahkan.')
            return redirect('/projects/task/draft')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal menambahkan project.'}, status=500)
            messages.error(request, 'Gagal menambahkan project.')
            return redirect('/projects/task/draft')

class DraftEditView(View):
    def get(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT partner_id, partner_name, partner_contact, partner_region,
                   request_date, pic, status, adnetwork, mdd
            FROM data_media_partner
            WHERE partner_id = %s
            LIMIT 1
        """
        partner = None
        if db.execute_query(sql, (partner_id,)):
            partner = db.cur_hris.fetchone()
        users = []
        q_users = """
            SELECT user_alias
            FROM app_users
            WHERE user_st = '1'
            ORDER BY user_alias ASC
        """
        if db.execute_query(q_users):
            users = db.cur_hris.fetchall() or []
        domains = []
        q_domains = """
            SELECT domain_id, domain AS domain_label
            FROM data_domains
            ORDER BY domain ASC
        """
        if db.execute_query(q_domains):
            domains = db.cur_hris.fetchall() or []
        selected_domain_ids = []
        q_rel = """
            SELECT domain_id
            FROM data_media_partner_domain
            WHERE partner_id = %s
        """
        if db.execute_query(q_rel, (partner_id,)):
            rows = db.cur_hris.fetchall() or []
            for r in rows:
                val = None
                try:
                    val = r.get('domain_id')
                except AttributeError:
                    try:
                        val = r[0]
                    except Exception:
                        val = None
                if val is None:
                    continue
                try:
                    selected_domain_ids.append(int(str(val)))
                except ValueError:
                    continue
        statuses = ['draft','waiting','canceled','rejected','completed']
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partner': partner,
            'users': users,
            'domains': domains,
            'selected_domains': selected_domain_ids,
            'statuses': statuses,
        }
        return render(request, 'task/draft/edit.html', context)
    def post(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        partner_name = request.POST.get('partner_name', '').strip()
        partner_contact = request.POST.get('partner_contact', '').strip()
        partner_region = request.POST.get('partner_region', '').strip()
        request_date_raw = request.POST.get('request_date', '').strip()
        pic = request.POST.get('pic', '').strip()
        adnetwork = request.POST.get('adnetwork', '').strip() or 'adx'
        status = request.POST.get('status', '').strip() or 'draft'
        update_action = request.POST.get('update', '').strip()
        domain_ids = request.POST.getlist('domains')
        try:
            fixed = request_date_raw.strip()
            request_date = datetime.strptime(fixed, '%Y-%m-%d') if fixed else None
        except ValueError:
            request_date = None
        is_ajax = (request.headers.get('x-requested-with') == 'XMLHttpRequest') or (
            request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'
        )
        if not partner_name:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Nama partner wajib diisi.'}, status=400)
            messages.error(request, 'Nama partner wajib diisi.')
            return redirect('/projects/task/draft')
        # if not domain_ids:
        #     if is_ajax:
        #         return JsonResponse({'status': False, 'message': 'Domain wajib dipilih.'}, status=400)
        #     messages.error(request, 'Domain wajib dipilih.')
        #     return redirect('/projects/task/draft')
        if update_action == 'send':
            status = 'waiting'
        sql = """
            UPDATE data_media_partner SET
                partner_name = %s,
                partner_contact = %s,
                partner_region = %s,
                request_date = %s,
                pic = %s,
                adnetwork = %s,
                status = %s,
                mdb = %s,
                mdb_name = %s,
                mdd = NOW()
            WHERE partner_id = %s
        """
        params = (
            partner_name,
            partner_contact or None,
            partner_region or None,
            request_date,
            pic or None,
            adnetwork,
            status,
            admin.get('user_id', ''),
            admin.get('user_alias', ''),
            partner_id,
        )
        if db.execute_query(sql, params):
            db.commit()
            db.execute_query("DELETE FROM data_media_partner_domain WHERE partner_id = %s", (partner_id,))
            db.commit()
            sql_rel = """
                INSERT INTO data_media_partner_domain (partner_id, domain_id)
                VALUES (%s, %s)
            """
            for did in domain_ids:
                val = (str(did or '').strip())
                if not val:
                    continue
                try:
                    num = int(val)
                except ValueError:
                    continue
                db.execute_query(sql_rel, (partner_id, num))
            db.commit()
            if update_action == 'send':
                now = datetime.now()
                base = now.strftime('%Y%m%d%H%M%S')
                rnd = f"{random.randint(0, 9999):04d}"
                process_id = f"PR{base}{rnd}"[:20]
                admin_id = str(admin.get('user_id', ''))[:10]
                admin_alias = admin.get('user_alias', '')
                sql_proc = """
                    INSERT INTO data_media_process
                    (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """
                recipients = request.POST.getlist('recipients[]') or request.POST.getlist('recipients') or []
                catatan_text = ", ".join([v for v in recipients if v]) if recipients else ""
                catatan_val = f"Assigned to: {catatan_text}" if catatan_text else None
                params_proc = (
                    process_id,
                    partner_id,
                    '1001',
                    None,
                    'waiting',
                    'process',
                    catatan_val,
                    admin_id,
                    admin_alias,
                )
                db.execute_query(sql_proc, params_proc)
                db.commit()
                notify_email_raw = (request.POST.get('notify_email') or '').strip().lower()
                notify_whatsapp_raw = (request.POST.get('notify_whatsapp') or '').strip().lower()
                notify_email = notify_email_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
                notify_whatsapp = notify_whatsapp_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
                subdomains = []
                try:
                    sql_sub = """
                        SELECT DISTINCT CONCAT(s.subdomain, '.', d.domain) AS website_name
                        FROM data_media_partner_domain pd
                        JOIN data_domains d ON d.domain_id = pd.domain_id
                        LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                        WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL
                    """
                    if db.execute_query(sql_sub, (partner_id,)):
                        for rr in (db.cur_hris.fetchall() or []):
                            try:
                                nm = rr.get('website_name')
                            except AttributeError:
                                nm = rr[0]
                            if nm:
                                subdomains.append(str(nm))
                except Exception:
                    subdomains = []
                sub_text = "<br>".join(subdomains) if subdomains else "-"
                subject = "Task untuk setting server"
                body = f"Task untuk setting server: <br>{sub_text}<br><br>Tolong setting untuk server, Cloudflare, dan WordPress beserta plugin-nya."
                if notify_email and recipients:
                    try:
                        send_mail_notification(
                            list(dict.fromkeys([v for v in recipients if v])),
                            subject,
                            body
                        )
                    except Exception:
                        pass
                if notify_whatsapp and recipients:
                    try:
                        send_whatsapp_notification(list(dict.fromkeys([v for v in recipients if v])), body)
                    except Exception:
                        pass
            if is_ajax:
                if update_action == 'send':
                    res = {'status': True, 'message': 'Berhasil diupdate dan dikirim.'}
                    res['process_id'] = process_id
                else:
                    res = {'status': True, 'message': 'Berhasil diupdate.'}
                return JsonResponse(res)
            messages.success(request, 'Draft partner berhasil diperbarui.')
            return redirect('/projects/task/draft')
        else:
            if is_ajax:
                return JsonResponse({'status': False, 'message': 'Gagal memperbarui draft partner.'}, status=500)
            messages.error(request, 'Gagal memperbarui draft partner.')
            return redirect('/projects/task/draft')

class DraftDeleteView(View):
    def post(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        db = data_mysql()
        sql = "DELETE FROM data_media_partner WHERE partner_id = %s"
        if db.execute_query(sql, (partner_id,)):
            db.commit()
            messages.success(request, 'Draft partner berhasil dihapus.')
        else:
            messages.error(request, 'Gagal menghapus draft partner.')
        return redirect('/projects/task/draft')

class MonitoringIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   mp.partner_name,
                   mp.partner_contact,
                   mp.request_date,
                   mp.pic,
                   df.task_name,
                   latest.catatan,
                   (
                     SELECT GROUP_CONCAT(DISTINCT CONCAT(s.subdomain, '.', d.domain) ORDER BY d.domain ASC, s.subdomain ASC SEPARATOR ', ')
                     FROM data_media_partner_domain pd
                     JOIN data_domains d ON d.domain_id = pd.domain_id
                     LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                     WHERE pd.partner_id = mp.partner_id AND s.subdomain_id IS NOT NULL
                   ) AS subdomain_list
            FROM data_media_partner mp
            LEFT JOIN (
                SELECT p.partner_id, p.flow_id, p.mdd, p.catatan
                FROM data_media_process p
                JOIN (
                    SELECT partner_id, MAX(COALESCE(mdd, '0000-00-00 00:00:00')) AS max_mdd
                    FROM data_media_process
                    GROUP BY partner_id
                ) t ON t.partner_id = p.partner_id AND COALESCE(p.mdd, '0000-00-00 00:00:00') = t.max_mdd
                WHERE p.process_st = 'waiting'
            ) latest ON latest.partner_id = mp.partner_id
            LEFT JOIN data_flow df ON df.flow_id = latest.flow_id
            WHERE mp.status = 'waiting'
            ORDER BY COALESCE(mp.mdd, mp.request_date) DESC
        """
        items = []
        if db.execute_query(sql):
            items = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'items': items,
        }
        return render(request, 'task/monitoring/index.html', context)

class MonitoringDetailView(View):
    def get(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        partner = None
        sql_partner = """
            SELECT partner_id, partner_name, partner_contact, partner_region, request_date, pic
            FROM data_media_partner
            WHERE partner_id = %s
            LIMIT 1
        """
        if db.execute_query(sql_partner, (partner_id,)):
            partner = db.cur_hris.fetchone()
        domains = []
        sql_domains = """
            SELECT
                d.domain_id,
                d.domain,
                ds.hostname,
                ds.label,
                COALESCE(prv1.provider, d.provider) AS provider_name,
                COALESCE(prv2.provider, d.registrar) AS registrar_name,
                w.domain AS w_domain,
                w.website,
                w.website_user,
                w.website_pass,
                w.article_status,
                w.article_deadline
            FROM data_media_partner_domain pd
            JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_servers ds ON ds.server_id = d.server_id
            LEFT JOIN data_website w ON w.website_id = d.website_id
            LEFT JOIN data_server_registrar_provider prv1 ON prv1.provider = d.provider
            LEFT JOIN data_server_registrar_provider prv2 ON prv2.provider = d.registrar
            WHERE pd.partner_id = %s
            ORDER BY d.domain ASC
        """
        if db.execute_query(sql_domains, (partner_id,)):
            domains = db.cur_hris.fetchall() or []
        subdomains = []
        sql_subdomains = """
            SELECT
                s.subdomain_id,
                s.subdomain,
                d.domain AS domain,
                s.cloudflare,
                s.public_ipv4,
                ds.hostname,
                ds.label,
                w.domain AS w_domain,
                w.website,
                w.website_user,
                w.website_pass,
                w.article_status,
                w.article_deadline,
                s.tracker,
                s.tracker_params,
                s.plugin_setup,
                s.plugin_lp,
                s.plugin_params
            FROM data_media_partner_domain pd
            JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website w ON w.website_id = s.website_id
            LEFT JOIN data_servers ds ON ds.public_ipv4 = s.public_ipv4
            WHERE pd.partner_id = %s
            ORDER BY s.subdomain ASC
        """
        if db.execute_query(sql_subdomains, (partner_id,)):
            subdomains = db.cur_hris.fetchall() or []
        fb_ads = []
        sql_fb_ads = """
            SELECT
                f.ads_id,
                f.domain_id,
                f.subdomain_id,
                COALESCE(ma1.account_name, ma2.account_name) AS account_name,
                f.fanpage,
                f.interest,
                f.country,
                f.daily_budget,
                f.status,
                d.domain AS domain,
                CONCAT(s.subdomain, '.', d2.domain) AS subdomain_name
            FROM data_media_fb_ads f
            LEFT JOIN data_domains d ON d.domain_id = f.domain_id
            LEFT JOIN data_subdomain s ON s.subdomain_id = f.subdomain_id
            LEFT JOIN data_domains d2 ON d2.domain_id = s.domain_id
            LEFT JOIN master_account_ads ma1 ON ma1.account_ads_id = f.account_ads_id_1
            LEFT JOIN master_account_ads ma2 ON ma2.account_ads_id = f.account_ads_id_2
            WHERE f.domain_id IN (
                SELECT domain_id FROM data_media_partner_domain WHERE partner_id = %s
            )
            OR f.subdomain_id IN (
                SELECT s2.subdomain_id
                FROM data_subdomain s2
                JOIN data_media_partner_domain pd2 ON pd2.domain_id = s2.domain_id
                WHERE pd2.partner_id = %s
            )
            ORDER BY COALESCE(f.mdd, '0000-00-00 00:00:00') DESC
        """
        if db.execute_query(sql_fb_ads, (partner_id, partner_id)):
            fb_ads = db.cur_hris.fetchall() or []
        processes = []
        sql_processes = """
            SELECT
                p.flow_id,
                df.task_name,
                p.process_st,
                p.action_st,
                p.catatan,
                p.mdb_name,
                p.mdd,
                p.mdb_finish_name,
                p.mdd_finish
            FROM data_media_process p
            LEFT JOIN data_flow df ON df.flow_id = p.flow_id
            WHERE p.partner_id = %s
            ORDER BY COALESCE(p.mdd, '0000-00-00 00:00:00') ASC
        """
        if db.execute_query(sql_processes, (partner_id,)):
            processes = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partner': partner,
            'domains': domains,
            'subdomains': subdomains,
            'fb_ads': fb_ads,
            'processes': processes,
        }
        return render(request, 'task/monitoring/detail.html', context)

class ActiveIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT
                w.website_id AS website_id,
                s.*, MAX(dmp.pic) AS pic, MAX(dmp.partner_name) AS partner_name, MAX(dmp.request_date) AS request_date, MAX(dmp.partner_contact) AS partner_contact,
                MAX(dmp.partner_id) AS partner_id,
                MAX(dmp.status) AS status,
                CONCAT(s.subdomain, '.', d.domain) AS website_name,
                s.public_ipv4, MAX(ds.hostname) AS hostname, MAX(ds.provider) AS provider, 
                MAX(ds.vcpu_count) AS vcpu, MAX(ds.memory_gb) AS memory_gb,
                w.website_user AS website_user,
                w.website_pass AS website_pass,
                (SELECT COUNT(*) FROM data_website_niche k WHERE k.subdomain_id = s.subdomain_id) AS keyword_count,
                MAX(a.account_name) AS fb_account,
                MAX(b.fanpage) AS fb_fanpage,
                MAX(b.daily_budget) AS fb_daily_budget,
                MAX(b.country) AS fb_country,
                MAX(b.interest) AS fb_interest,
                CONCAT_WS(', ',
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn
                        WHERE FIND_IN_SET(mn.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn.tier AS CHAR), 'Tier ', '') = '1'
                    ) THEN 'Tier 1' END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn2
                        WHERE FIND_IN_SET(mn2.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn2.tier AS CHAR), 'Tier ', '') = '2'
                    ) THEN 'Tier 2' END
                ) AS country_tier,
                MAX(n.niche) AS niche
            FROM data_website w
            INNER JOIN data_subdomain s ON s.website_id = w.website_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            INNER JOIN data_media_partner_domain dmpd ON d.domain_id = dmpd.domain_id
            INNER JOIN data_media_partner dmp ON dmpd.partner_id = dmp.partner_id
            LEFT JOIN data_website_niche wn ON wn.subdomain_id = s.subdomain_id
            LEFT JOIN data_niche n ON n.niche_id = wn.niche_id
            LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
            LEFT JOIN master_account_ads a ON a.account_ads_id = COALESCE(NULLIF(b.account_ads_id_2, ''), b.account_ads_id_1)
            LEFT JOIN data_servers ds ON s.public_ipv4 = ds.public_ipv4
            WHERE s.subdomain_id IS NOT NULL AND dmp.status = 'completed'
            GROUP BY s.subdomain_id
            ORDER BY s.subdomain ASC, COALESCE(w.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql):
            rows = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'websites': rows,
        }
        return render(request, 'task/active/index.html', context)
class NonactiveIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT
                w.website_id AS website_id,
                s.*, MAX(dmp.pic) AS pic, MAX(dmp.partner_name) AS partner_name, MAX(dmp.request_date) AS request_date, MAX(dmp.partner_contact) AS partner_contact,
                MAX(dmp.partner_id) AS partner_id,
                MAX(dmp.status) AS status,
                CONCAT(s.subdomain, '.', d.domain) AS website_name,
                s.public_ipv4, MAX(ds.hostname) AS hostname, MAX(ds.provider) AS provider, 
                MAX(ds.vcpu_count) AS vcpu, MAX(ds.memory_gb) AS memory_gb,
                w.website_user AS website_user,
                w.website_pass AS website_pass,
                (SELECT COUNT(*) FROM data_website_niche k WHERE k.subdomain_id = s.subdomain_id) AS keyword_count,
                MAX(a.account_name) AS fb_account,
                MAX(b.fanpage) AS fb_fanpage,
                MAX(b.daily_budget) AS fb_daily_budget,
                MAX(b.country) AS fb_country,
                MAX(b.interest) AS fb_interest,
                CONCAT_WS(', ',
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn
                        WHERE FIND_IN_SET(mn.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn.tier AS CHAR), 'Tier ', '') = '1'
                    ) THEN 'Tier 1' END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn2
                        WHERE FIND_IN_SET(mn2.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn2.tier AS CHAR), 'Tier ', '') = '2'
                    ) THEN 'Tier 2' END
                ) AS country_tier,
                MAX(n.niche) AS niche
            FROM data_website w
            INNER JOIN data_subdomain s ON s.website_id = w.website_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            INNER JOIN data_media_partner_domain dmpd ON d.domain_id = dmpd.domain_id
            INNER JOIN data_media_partner dmp ON dmpd.partner_id = dmp.partner_id
            LEFT JOIN data_website_niche wn ON wn.subdomain_id = s.subdomain_id
            LEFT JOIN data_niche n ON n.niche_id = wn.niche_id
            LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
            LEFT JOIN master_account_ads a ON a.account_ads_id = COALESCE(NULLIF(b.account_ads_id_2, ''), b.account_ads_id_1)
            LEFT JOIN data_servers ds ON s.public_ipv4 = ds.public_ipv4
            WHERE s.subdomain_id IS NOT NULL AND dmp.status = 'off'
            GROUP BY s.subdomain_id
            ORDER BY s.subdomain ASC, COALESCE(w.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql):
            rows = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'websites': rows,
        }
        return render(request, 'task/nonactive/index.html', context)

class ProfitSharingIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        selected_periode = (request.GET.get('periode') or '').strip()
        normalized_periode = ''
        if selected_periode:
            try:
                parts = selected_periode.split('-')
                if len(parts) == 2:
                    y = int(parts[0])
                    m = int(parts[1])
                    if 1 <= m <= 12:
                        normalized_periode = f"{y:04d}-{m:02d}"
            except Exception:
                normalized_periode = ''
        if not normalized_periode:
            now = datetime.now()
            normalized_periode = now.strftime('%Y-%m')
        db = data_mysql()
        p1_filter = ""
        p2_filter = ""
        params = []
        if normalized_periode:
            p2_filter = " WHERE DATE_FORMAT(periode, '%%Y-%%m') = %s "
            p1_filter = " AND DATE_FORMAT(p1.periode, '%%Y-%%m') = %s "
            params.extend([normalized_periode, normalized_periode])
        sql = f"""
            SELECT
                mp.partner_id,
                mp.partner_name,
                mp.partner_contact,
                mp.status,
                mp.request_date,
                s.subdomain_id,
                CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                d.domain_id AS domain_id,
                d.domain AS domain_name,
                sp.content,
                sp.periode,
                sp.mcm_revenue_share
            FROM data_media_partner mp
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN (
                SELECT p1.partner_id, p1.domain_id, p1.subdomain_id, p1.mcm_revenue_share, p1.periode, p1.content, p1.mdd
                FROM data_sharing_profit p1
                INNER JOIN (
                    SELECT partner_id, domain_id, subdomain_id, periode,
                           MAX(COALESCE(mdd, '0000-00-00 00:00:00')) AS max_mdd
                    FROM data_sharing_profit
                    {p2_filter}
                    GROUP BY partner_id, domain_id, subdomain_id, periode
                ) p2 ON p1.partner_id = p2.partner_id
                   AND p1.domain_id <=> p2.domain_id
                   AND p1.subdomain_id <=> p2.subdomain_id
                   AND p1.periode <=> p2.periode
                   AND COALESCE(p1.mdd, '0000-00-00 00:00:00') = p2.max_mdd
                {p1_filter}
            ) sp ON sp.partner_id = mp.partner_id
                AND (
                    (sp.domain_id IS NOT NULL AND sp.domain_id = d.domain_id AND sp.subdomain_id IS NULL)
                    OR (sp.subdomain_id IS NOT NULL AND sp.subdomain_id = s.subdomain_id)
                )
            WHERE mp.status IN ('completed','off')
            ORDER BY mp.partner_name ASC, COALESCE(sp.periode, '0000-00-00') DESC, d.domain ASC, s.subdomain ASC
        """
        rows = []
        if params:
            ok = db.execute_query(sql, params)
        else:
            ok = db.execute_query(sql)
        if ok:
            rows = db.cur_hris.fetchall() or []
        def as_number(val):
            try:
                if val is None or val == '':
                    return 0
                return float(val)
            except Exception:
                return 0
        def pick_value(data, keys):
            if not isinstance(data, dict):
                return None
            for k in keys:
                if k in data:
                    return data.get(k)
            return None
        def calc_metrics(adsense_num, modal_num, expenses_num):
            try:
                base = adsense_num - modal_num
                media_calc = base * 0.2
                fix_calc = (adsense_num - media_calc) - modal_num - expenses_num
            except Exception:
                media_calc = 0
                fix_calc = 0
            total_calc = fix_calc + modal_num
            return media_calc, fix_calc, total_calc
        partners_map = {}
        for r in rows:
            try:
                pid = r.get('partner_id')
                pname = r.get('partner_name')
                pcontact = r.get('partner_contact')
                status = r.get('status')
                request_date = r.get('request_date')
                subdomain_name = r.get('subdomain_name')
                domain_id = r.get('domain_id')
                domain_name = r.get('domain_name')
                content_raw = r.get('content')
                periode = r.get('periode')
                mcm_revenue_share = r.get('mcm_revenue_share')
            except AttributeError:
                pid = r[0] if len(r) > 0 else None
                pname = r[1] if len(r) > 1 else None
                pcontact = r[2] if len(r) > 2 else None
                status = r[3] if len(r) > 3 else None
                request_date = r[4] if len(r) > 4 else None
                subdomain_name = r[6] if len(r) > 6 else None
                domain_id = r[7] if len(r) > 7 else None
                domain_name = r[8] if len(r) > 8 else None
                content_raw = r[9] if len(r) > 9 else None
                periode = r[10] if len(r) > 10 else None
                mcm_revenue_share = r[11] if len(r) > 11 else None
            if not pid:
                continue
            g = partners_map.get(pid)
            if not g:
                g = {
                    'partner_id': pid,
                    'partner_name': pname,
                    'partner_contact': pcontact,
                    'status': status,
                    'request_date': request_date,
                    'rows': [],
                }
                partners_map[pid] = g
            content = {}
            if isinstance(content_raw, dict):
                content = content_raw
            elif content_raw:
                try:
                    content = json.loads(content_raw)
                except Exception:
                    content = {}
            adsense_val = pick_value(content, ['adsense_adx_earning', 'adsense', 'adsense_earning', 'earning_adsense'])
            modal_val = pick_value(content, ['modal', 'modal_cost'])
            expenses_val = pick_value(content, ['total_expenses', 'expenses', 'total_expense'])
            adsense_num = as_number(adsense_val)
            modal_num = as_number(modal_val)
            expenses_num = as_number(expenses_val)
            media_calc, fix_calc, total_calc = calc_metrics(adsense_num, modal_num, expenses_num)
            g['rows'].append({
                'subdomain_name': subdomain_name,
                'domain': domain_name,
                'domain_id': domain_id,
                'subdomain_id': r.get('subdomain_id') if isinstance(r, dict) else (r[4] if len(r) > 4 else None),
                'periode': periode,
                'adsense_adx_earning': adsense_num,
                'media': media_calc,
                'modal': modal_num,
                'total_expenses': expenses_num,
                'fix_earning': fix_calc,
                'total': total_calc,
                'mcm_revenue_share': mcm_revenue_share,
            })
        partners = list(partners_map.values())
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partners': partners,
            'selected_periode': normalized_periode,
        }
        return render(request, 'task/invoice/index.html', context)

class ProfitSharingSaveView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        domain_id = (request.POST.get('domain_id') or '').strip()
        subdomain_id = (request.POST.get('subdomain_id') or '').strip()
        field = (request.POST.get('name') or '').strip()
        raw_value = request.POST.get('value')
        if field not in ['adsense_adx_earning', 'modal', 'total_expenses', 'mcm_revenue_share']:
            return JsonResponse({'status': False, 'message': 'Field tidak valid.'}, status=400)
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner tidak valid.'}, status=400)
        if not domain_id and not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Domain/Subdomain tidak valid.'}, status=400)
        def parse_number(val):
            try:
                if val is None:
                    return 0
                s = str(val)
                if s == '':
                    return 0
                cleaned = re.sub(r'[^0-9\.\-]', '', s)
                return float(cleaned) if cleaned not in ['', '-', '.'] else 0
            except Exception:
                return 0
        value_num = parse_number(raw_value)
        raw_periode = (request.POST.get('periode') or '').strip()
        periode = None
        if raw_periode:
            try:
                parts = raw_periode.split('-')
                if len(parts) == 2:
                    y = int(parts[0])
                    m = int(parts[1])
                    if 1 <= m <= 12:
                        last_day = calendar.monthrange(y, m)[1]
                        periode = datetime(y, m, last_day).date()
            except Exception:
                periode = None
        if not periode:
            now = datetime.now()
            last_day = calendar.monthrange(now.year, now.month)[1]
            periode = datetime(now.year, now.month, last_day).date()
        db = data_mysql()
        exists_id = None
        existing_content = {}
        if subdomain_id:
            sql_find = """
                SELECT id_sharing_profit, content
                FROM data_sharing_profit
                WHERE partner_id = %s AND subdomain_id = %s AND periode = %s
                ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql_find, (partner_id, subdomain_id, periode)):
                row = db.cur_hris.fetchone() or {}
                exists_id = row.get('id_sharing_profit') if isinstance(row, dict) else (row[0] if row else None)
                content_raw = row.get('content') if isinstance(row, dict) else (row[1] if row else None)
                if content_raw:
                    try:
                        existing_content = json.loads(content_raw) if not isinstance(content_raw, dict) else content_raw
                    except Exception:
                        existing_content = {}
        else:
            sql_find = """
                SELECT id_sharing_profit, content
                FROM data_sharing_profit
                WHERE partner_id = %s AND domain_id = %s AND (subdomain_id IS NULL OR subdomain_id = 0) AND periode = %s
                ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql_find, (partner_id, domain_id, periode)):
                row = db.cur_hris.fetchone() or {}
                exists_id = row.get('id_sharing_profit') if isinstance(row, dict) else (row[0] if row else None)
                content_raw = row.get('content') if isinstance(row, dict) else (row[1] if row else None)
                if content_raw:
                    try:
                        existing_content = json.loads(content_raw) if not isinstance(content_raw, dict) else content_raw
                    except Exception:
                        existing_content = {}
        existing_content = existing_content if isinstance(existing_content, dict) else {}
        prev_mcm = parse_number(existing_content.get('mcm_revenue_share', 0))
        base_adsense = parse_number(existing_content.get('adsense_adx_earning_raw', existing_content.get('adsense_adx_earning', 0)))
        updated_adsense = None
        if field == 'adsense_adx_earning':
            base_adsense = value_num
            existing_content['adsense_adx_earning_raw'] = base_adsense
            existing_content['adsense_adx_earning'] = base_adsense
            mcm_percent = parse_number(existing_content.get('mcm_revenue_share', 0))
            if mcm_percent < 0:
                mcm_percent = 0
            if mcm_percent > 100:
                mcm_percent = 100
            if mcm_percent > 0:
                updated_adsense = base_adsense - (base_adsense * (mcm_percent / 100))
                existing_content['adsense_adx_earning'] = updated_adsense
        elif field == 'mcm_revenue_share':
            existing_content['mcm_revenue_share'] = value_num
            if not existing_content.get('adsense_adx_earning_raw'):
                current_adsense = parse_number(existing_content.get('adsense_adx_earning', 0))
                if prev_mcm > 0 and prev_mcm < 100:
                    base_adsense = current_adsense / (1 - (prev_mcm / 100))
                else:
                    base_adsense = current_adsense
            existing_content['adsense_adx_earning_raw'] = base_adsense
            mcm_percent = parse_number(value_num)
            if mcm_percent < 0:
                mcm_percent = 0
            if mcm_percent > 100:
                mcm_percent = 100
            updated_adsense = base_adsense - (base_adsense * (mcm_percent / 100))
            existing_content['adsense_adx_earning'] = updated_adsense
        else:
            existing_content[field] = value_num
        adsense_num = parse_number(existing_content.get('adsense_adx_earning', 0))
        modal_num = parse_number(existing_content.get('modal', 0))
        expenses_num = parse_number(existing_content.get('total_expenses', 0))
        base = adsense_num - modal_num
        media_calc = base * 0.2
        fix_calc = (adsense_num - media_calc) - modal_num - expenses_num
        total_calc = fix_calc + modal_num
        existing_content['media'] = media_calc
        existing_content['fix_earning'] = fix_calc
        existing_content['total'] = total_calc
        content_json = json.dumps(existing_content)
        mdb = str(admin.get('user_id', ''))[:36]
        mdb_name = admin.get('user_alias', '')
        if exists_id:
            update_fields = ["content=%s", "mdb=%s", "mdb_name=%s", "mdd=NOW()"]
            update_params = [content_json, mdb, mdb_name]
            if field == 'mcm_revenue_share':
                update_fields.append("mcm_revenue_share=%s")
                update_params.append(value_num)
            update_params.append(exists_id)
            ok = db.execute_query(
                f"""
                UPDATE data_sharing_profit
                SET {', '.join(update_fields)}
                WHERE id_sharing_profit=%s
                """,
                tuple(update_params)
            )
            if not ok:
                return JsonResponse({'status': False, 'message': 'Gagal menyimpan.'}, status=500)
            db.commit()
        else:
            insert_columns = ["partner_id", "domain_id", "subdomain_id", "content", "periode", "mdb", "mdb_name"]
            insert_values = [partner_id, domain_id or None, subdomain_id or None, content_json, periode, mdb, mdb_name]
            insert_placeholders = ["%s"] * len(insert_columns)
            if field == 'mcm_revenue_share':
                insert_columns.append("mcm_revenue_share")
                insert_values.append(value_num)
                insert_placeholders.append("%s")
            ok = db.execute_query(
                f"""
                INSERT INTO data_sharing_profit
                ({', '.join(insert_columns)}, mdd)
                VALUES ({', '.join(insert_placeholders)}, NOW())
                """,
                tuple(insert_values)
            )
            if not ok:
                return JsonResponse({'status': False, 'message': 'Gagal menyimpan.'}, status=500)
            db.commit()
        response_payload = {'status': True, 'value': value_num}
        if updated_adsense is not None:
            response_payload['updated_adsense_adx_earning'] = updated_adsense
        return JsonResponse(response_payload)

class ProfitSharingAdxAccountListView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, request):
        return management_views.AdxAccountListView().get(request)

class ProfitSharingRoiTrafficDomainView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, request):
        resp = management_views.RoiTrafficPerDomainDataView().get(request)
        try:
            if isinstance(resp, JsonResponse):
                payload = json.loads(resp.content)
                summary = (payload or {}).get('summary') or {}
                partner_id = (request.GET.get('partner_id') or '').strip()
                domain_id = (request.GET.get('domain_id') or '').strip()
                subdomain_id = (request.GET.get('subdomain_id') or '').strip()
                start_date = (request.GET.get('start_date') or '').strip()
                end_date = (request.GET.get('end_date') or '').strip()
                account_id = (request.GET.get('account_id') or '').strip()
                raw_mcm = request.GET.get('mcm_revenue_share')
                mcm_revenue_share = None
                if raw_mcm is not None:
                    raw_mcm = str(raw_mcm).strip()
                    if raw_mcm != '' and raw_mcm.lower() != 'null':
                        mcm_revenue_share = raw_mcm
                def parse_date(d):
                    try:
                        return datetime.strptime(d, '%Y-%m-%d').date()
                    except Exception:
                        return None
                periode_date = parse_date(end_date) or parse_date(start_date)
                if not periode_date:
                    now = datetime.now()
                    first_current = datetime(now.year, now.month, 1)
                    last_prev = first_current - timedelta(days=1)
                    periode_date = last_prev.date()
                else:
                    last_day = calendar.monthrange(periode_date.year, periode_date.month)[1]
                    periode_date = datetime(periode_date.year, periode_date.month, last_day).date()
                def parse_number(val):
                    try:
                        if val is None:
                            return 0
                        s = str(val)
                        if s == '':
                            return 0
                        cleaned = re.sub(r'[^0-9\.\-]', '', s)
                        return float(cleaned) if cleaned not in ['', '-', '.'] else 0
                    except Exception:
                        return 0
                total_revenue = parse_number(summary.get('total_revenue', 0))
                total_spend = parse_number(summary.get('total_spend', 0))
                if partner_id and (domain_id or subdomain_id):
                    admin = request.session.get('hris_admin', {})
                    db = data_mysql()
                    exists_id = None
                    existing_content = {}
                    if subdomain_id:
                        sql_find = """
                            SELECT id_sharing_profit, content
                            FROM data_sharing_profit
                            WHERE partner_id = %s AND subdomain_id = %s AND periode = %s
                            ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                            LIMIT 1
                        """
                        if db.execute_query(sql_find, (partner_id, subdomain_id, periode_date)):
                            row = db.cur_hris.fetchone() or {}
                            exists_id = row.get('id_sharing_profit') if isinstance(row, dict) else (row[0] if row else None)
                            content_raw = row.get('content') if isinstance(row, dict) else (row[1] if row else None)
                            if content_raw:
                                try:
                                    existing_content = json.loads(content_raw) if not isinstance(content_raw, dict) else content_raw
                                except Exception:
                                    existing_content = {}
                    else:
                        sql_find = """
                            SELECT id_sharing_profit, content
                            FROM data_sharing_profit
                            WHERE partner_id = %s AND domain_id = %s AND (subdomain_id IS NULL OR subdomain_id = 0) AND periode = %s
                            ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                            LIMIT 1
                        """
                        if db.execute_query(sql_find, (partner_id, domain_id, periode_date)):
                            row = db.cur_hris.fetchone() or {}
                            exists_id = row.get('id_sharing_profit') if isinstance(row, dict) else (row[0] if row else None)
                            content_raw = row.get('content') if isinstance(row, dict) else (row[1] if row else None)
                            if content_raw:
                                try:
                                    existing_content = json.loads(content_raw) if not isinstance(content_raw, dict) else content_raw
                                except Exception:
                                    existing_content = {}
                    existing_content = existing_content if isinstance(existing_content, dict) else {}
                    existing_content['adsense_adx_earning'] = total_revenue
                    existing_content['adsense_adx_earning_raw'] = total_revenue
                    existing_content['modal'] = total_spend
                    if mcm_revenue_share is not None:
                        existing_content['mcm_revenue_share'] = parse_number(mcm_revenue_share)
                    adsense_num = parse_number(existing_content.get('adsense_adx_earning', 0))
                    modal_num = parse_number(existing_content.get('modal', 0))
                    expenses_num = parse_number(existing_content.get('total_expenses', 0))
                    base = adsense_num - modal_num
                    media_calc = base * 0.2
                    fix_calc = (adsense_num - media_calc) - modal_num - expenses_num
                    total_calc = fix_calc + modal_num
                    existing_content['media'] = media_calc
                    existing_content['fix_earning'] = fix_calc
                    existing_content['total'] = total_calc
                    content_json = json.dumps(existing_content)
                    mdb = str(admin.get('user_id', ''))[:36]
                    mdb_name = admin.get('user_alias', '')
                    if exists_id:
                        update_fields = ["content=%s", "mdb=%s", "mdb_name=%s", "mdd=NOW()"]
                        update_params = [content_json, mdb, mdb_name]
                        if account_id:
                            update_fields.append("account_id=%s")
                            update_params.append(account_id)
                        if mcm_revenue_share is not None:
                            update_fields.append("mcm_revenue_share=%s")
                            update_params.append(mcm_revenue_share)
                        update_params.append(exists_id)
                        db.execute_query(
                            f"""
                            UPDATE data_sharing_profit
                            SET {', '.join(update_fields)}
                            WHERE id_sharing_profit=%s
                            """,
                            tuple(update_params)
                        )
                        db.commit()
                    else:
                        insert_columns = ["partner_id", "domain_id", "subdomain_id", "content", "periode", "mdb", "mdb_name"]
                        insert_values = [partner_id, domain_id or None, subdomain_id or None, content_json, periode_date, mdb, mdb_name]
                        insert_placeholders = ["%s"] * len(insert_columns)
                        if account_id:
                            insert_columns.append("account_id")
                            insert_values.append(account_id)
                            insert_placeholders.append("%s")
                        if mcm_revenue_share is not None:
                            insert_columns.append("mcm_revenue_share")
                            insert_values.append(mcm_revenue_share)
                            insert_placeholders.append("%s")
                        db.execute_query(
                            f"""
                            INSERT INTO data_sharing_profit
                            ({', '.join(insert_columns)}, mdd)
                            VALUES ({', '.join(insert_placeholders)}, NOW())
                            """,
                            tuple(insert_values)
                        )
                        db.commit()
                cleaned = {
                    'total_clicks_fb': summary.get('total_clicks_fb', 0),
                    'total_clicks_adx': summary.get('total_clicks_adx', 0),
                    'total_spend': summary.get('total_spend', 0),
                    'roi_nett': summary.get('roi_nett', 0),
                    'total_revenue': summary.get('total_revenue', 0),
                }
                return JsonResponse({
                    'status': payload.get('status', True),
                    'message': payload.get('message', ''),
                    'summary': cleaned
                }, safe=False)
        except Exception:
            return resp
        return resp

@method_decorator(xframe_options_sameorigin, name='dispatch')
class ProfitSharingInvoicePdfView(View):
    def dispatch(self, request, *args, **kwargs):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        return super().dispatch(request, *args, **kwargs)
    def get(self, request):
        partner_id = (request.GET.get('partner_id') or '').strip()
        domain_id = (request.GET.get('domain_id') or '').strip()
        subdomain_id = (request.GET.get('subdomain_id') or '').strip()
        raw_periode = (request.GET.get('periode') or '').strip()
        if not partner_id or (not domain_id and not subdomain_id):
            return HttpResponseBadRequest('Parameter tidak lengkap.')
        periode = None
        if raw_periode:
            try:
                parts = raw_periode.split('-')
                if len(parts) == 2:
                    y = int(parts[0])
                    m = int(parts[1])
                    if 1 <= m <= 12:
                        last_day = calendar.monthrange(y, m)[1]
                        periode = datetime(y, m, last_day).date()
            except Exception:
                periode = None
        if not periode:
            now = datetime.now()
            last_day = calendar.monthrange(now.year, now.month)[1]
            periode = datetime(now.year, now.month, last_day).date()
        def parse_number(val):
            try:
                if val is None:
                    return 0
                s = str(val)
                if s == '':
                    return 0
                cleaned = re.sub(r'[^0-9\.\-]', '', s)
                return float(cleaned) if cleaned not in ['', '-', '.'] else 0
            except Exception:
                return 0
        def pick_value(data, keys):
            if not isinstance(data, dict):
                return None
            for k in keys:
                if k in data:
                    return data.get(k)
            return None
        def calc_metrics(adsense_num, modal_num, expenses_num):
            try:
                base = adsense_num - modal_num
                media_calc = base * 0.2
                fix_calc = (adsense_num - media_calc) - modal_num - expenses_num
            except Exception:
                media_calc = 0
                fix_calc = 0
            total_calc = fix_calc + modal_num
            return media_calc, fix_calc, total_calc
        db = data_mysql()
        row = None
        if subdomain_id:
            sql = """
                SELECT sp.content, s.subdomain, d.domain, sp.account_id, ac.account_name
                FROM data_sharing_profit sp
                LEFT JOIN data_subdomain s ON s.subdomain_id = sp.subdomain_id
                LEFT JOIN data_domains d ON d.domain_id = s.domain_id
                LEFT JOIN app_credentials ac ON ac.account_id = sp.account_id
                WHERE sp.partner_id = %s AND sp.subdomain_id = %s AND sp.periode = %s
                ORDER BY COALESCE(sp.mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql, (partner_id, subdomain_id, periode)):
                row = db.cur_hris.fetchone()
        else:
            sql = """
                SELECT sp.content, d.domain, sp.account_id, ac.account_name
                FROM data_sharing_profit sp
                LEFT JOIN data_domains d ON d.domain_id = sp.domain_id
                LEFT JOIN app_credentials ac ON ac.account_id = sp.account_id
                WHERE sp.partner_id = %s AND sp.domain_id = %s AND (sp.subdomain_id IS NULL OR sp.subdomain_id = 0) AND sp.periode = %s
                ORDER BY COALESCE(sp.mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql, (partner_id, domain_id, periode)):
                row = db.cur_hris.fetchone()
        if not row:
            return HttpResponseBadRequest('Data tidak ditemukan.')
        try:
            if subdomain_id:
                content_raw = row.get('content')
                subdomain = row.get('subdomain')
                domain = row.get('domain')
                account_name = row.get('account_name')
            else:
                content_raw = row.get('content')
                subdomain = None
                domain = row.get('domain')
                account_name = row.get('account_name')
        except AttributeError:
            if subdomain_id:
                content_raw = row[0] if len(row) > 0 else None
                subdomain = row[1] if len(row) > 1 else None
                domain = row[2] if len(row) > 2 else None
                account_name = row[4] if len(row) > 4 else None
            else:
                content_raw = row[0] if len(row) > 0 else None
                subdomain = None
                domain = row[1] if len(row) > 1 else None
                account_name = row[3] if len(row) > 3 else None
        display_domain = ''
        if subdomain and domain:
            display_domain = f"{subdomain}.{domain}"
        elif domain:
            display_domain = str(domain)
        else:
            display_domain = '-'
        content = {}
        if isinstance(content_raw, dict):
            content = content_raw
        elif content_raw:
            try:
                content = json.loads(content_raw)
            except Exception:
                content = {}
        adsense_val = pick_value(content, ['adsense_adx_earning', 'adsense', 'adsense_earning', 'earning_adsense'])
        modal_val = pick_value(content, ['modal', 'modal_cost'])
        expenses_val = pick_value(content, ['total_expenses', 'expenses', 'total_expense'])
        adsense_num = parse_number(adsense_val)
        modal_num = parse_number(modal_val)
        expenses_num = parse_number(expenses_val)
        media_calc, fix_calc, total_calc = calc_metrics(adsense_num, modal_num, expenses_num)
        title = f"Invoice {periode.strftime('%Y-%m')}"
        account_label = str(account_name or '-')
        month_label = periode.strftime('%B')
        header_cells = ["Domain", "Adsense", "Media 20%", "Modal", "Total Expenses", "Fix Earning", "Total"]
        row_cells = [
            display_domain,
            f"Rp. {_format_pdf_number(adsense_num)}",
            f"Rp. {_format_pdf_number(media_calc)}",
            f"Rp. {_format_pdf_number(modal_num)}",
            f"Rp. {_format_pdf_number(expenses_num)}",
            f"Rp. {_format_pdf_number(fix_calc)}",
            f"Rp. {_format_pdf_number(total_calc)}",
        ]
        total_text = f"Rp. {_format_pdf_number(total_calc)}"
        pdf_bytes = _build_invoice_pdf_html(title, account_label, month_label, header_cells, row_cells, total_text, request)
        if not pdf_bytes:
            pdf_bytes = _build_invoice_pdf(title, account_label, month_label, header_cells, row_cells, total_text)
        filename_base = re.sub(r'[^A-Za-z0-9\.\-_]+', '-', display_domain).strip('-') or 'invoice'
        filename = f"{filename_base}-{periode.strftime('%Y-%m')}.pdf"
        response = HttpResponse(pdf_bytes, content_type='application/pdf')
        if str(request.GET.get('download') or '').strip() in ['1', 'true', 'yes']:
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
        else:
            response['Content-Disposition'] = f'inline; filename="{filename}"'
        return response
class TechnicalIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   mp.partner_name,
                   mp.partner_contact,
                   mp.partner_region,
                   mp.request_date,
                   mp.pic,
                   mp.status,
                   mp.mdd,
                   MAX(pr.process_id) AS process_id
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
            GROUP BY mp.partner_id
            ORDER BY COALESCE(mp.mdd, mp.request_date) DESC
        """
        partners = []
        if db.execute_query(sql, ('1001', '1001')):
            partners = db.cur_hris.fetchall() or []
        subs_by_partner = {}
        sql_subs = """
            SELECT mp.partner_id,
                   d.domain,
                   s.subdomain,
                   s.cloudflare,
                   s.public_ipv4,
                   s.subdomain_id,
                   w.website,
                   w.website_user,
                   w.website_pass
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.partner_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = %s
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website w ON w.website_id = s.website_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
            ORDER BY d.domain ASC, COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
        """
        subs_rows = []
        if db.execute_query(sql_subs, ('1001', '1001', '1001')):
            subs_rows = db.cur_hris.fetchall() or []
        # Group sub rows by partner_id and attach to partners
        for r in subs_rows:
            pid = None
            try:
                pid = r.get('partner_id')
            except AttributeError:
                try:
                    pid = r[0]
                except Exception:
                    pid = None
            if pid is None:
                continue
            subs_by_partner.setdefault(pid, []).append(r)
        # attach and set domain from first sub row if available
        for i, p in enumerate(partners):
            try:
                pid = p.get('partner_id')
            except AttributeError:
                try:
                    pid = p[0]
                except Exception:
                    pid = None
            rows = subs_by_partner.get(pid, [])
            # attach
            try:
                valid_rows = []
                for rr in rows:
                    sid = None
                    try:
                        sid = rr.get('subdomain_id')
                    except AttributeError:
                        try:
                            sid = rr[5]
                        except Exception:
                            sid = None
                    if sid and str(sid).strip().lower() not in ('none', ''):
                        valid_rows.append(rr)
                p['subrows'] = valid_rows
                if rows:
                    dname = rows[0].get('domain') if hasattr(rows[0], 'get') else None
                    p['domain'] = dname
                else:
                    p['domain'] = None
            except TypeError:
                pass
        statuses = ['draft','waiting','canceled','rejected','completed']
        providers = []
        q_providers = """
            SELECT provider
            FROM data_server_registrar_provider
            ORDER BY provider ASC
        """
        if db.execute_query(q_providers):
            providers = db.cur_hris.fetchall() or []
        niches = []
        q_niche = """
            SELECT niche_id, niche
            FROM data_niche
            ORDER BY niche ASC
        """
        if db.execute_query(q_niche):
            niches = db.cur_hris.fetchall() or []
        prompts = []
        q_prompts = """
            SELECT prompt_id, prompt
            FROM data_prompts
            ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC, prompt_id ASC
        """
        if db.execute_query(q_prompts):
            prompts = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partners': partners,
            'statuses': statuses,
            'providers': providers,
            'niches': niches,
            'prompts': prompts,
        }
        return render(request, 'task/technical/index.html', context)

class TechnicalPartnerDomainsView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        partner_id = (request.GET.get('partner_id') or '').strip()
        db = data_mysql()
        domains = []
        if partner_id:
            sql = """
                SELECT d.domain_id, d.domain
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                WHERE pd.partner_id = %s
                ORDER BY d.domain ASC
            """
            if db.execute_query(sql, (partner_id,)):
                domains = db.cur_hris.fetchall() or []
        return JsonResponse({'status': True, 'domains': domains})

class TechnicalLinkDomainView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        domain_id_raw = (request.POST.get('domain_id') or '').strip()
        try:
            domain_id = int(domain_id_raw)
        except Exception:
            domain_id = None
        if not partner_id or not domain_id:
            return JsonResponse({'status': False, 'message': 'Partner dan Domain wajib diisi.'}, status=400)
        db.execute_query(
            "INSERT IGNORE INTO data_media_partner_domain (partner_id, domain_id, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, NOW())",
            (partner_id, domain_id, admin.get('user_id',''), admin.get('user_alias',''))
        )
        db.commit()
        return JsonResponse({'status': True})

class TechnicalSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        recipients = request.POST.getlist('recipients[]') or request.POST.getlist('recipients') or []
        notify_email_raw = (request.POST.get('notify_email') or '').strip().lower()
        notify_whatsapp_raw = (request.POST.get('notify_whatsapp') or '').strip().lower()
        notify_email = notify_email_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        notify_whatsapp = notify_whatsapp_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        # pastikan proses saat ini ada
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        # update proses saat ini menjadi selesai
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb_finish=%s, mdb_finish_name=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', str(admin.get('user_id', ''))[:10], admin.get('user_alias', ''), process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        process_id = f"PR{base}{rnd}"[:20]
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        sql_proc = """
            INSERT INTO data_media_process
            (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        catatan_text = ", ".join([v for v in recipients if v]) if recipients else ""
        catatan_val = f"Assigned to: {catatan_text}" if catatan_text else None
        params_proc = (
            process_id,
            partner_id,
            '1002',
            None,
            'waiting',
            'process',
            catatan_val,
            admin_id,
            admin_alias,
        )
        if not db.execute_query(sql_proc, params_proc):
            return JsonResponse({'status': False, 'message': 'Gagal mengirim data ke proses berikutnya.'}, status=500)
        db.commit()
        domains = []
        try:
            sql_dom = """
                SELECT DISTINCT d.domain
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                WHERE pd.partner_id = %s
                ORDER BY d.domain ASC
            """
            if db.execute_query(sql_dom, (partner_id,)):
                for rr in (db.cur_hris.fetchall() or []):
                    try:
                        nm = rr.get('domain')
                    except AttributeError:
                        nm = rr[0]
                    if nm:
                        domains.append(str(nm))
        except Exception:
            domains = []
        websites = []
        try:
            sql_web = """
                SELECT DISTINCT w.website, w.website_user, w.website_pass
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                LEFT JOIN data_website w ON w.website_id = s.website_id
                WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL
            """
            if db.execute_query(sql_web, (partner_id,)):
                for rr in (db.cur_hris.fetchall() or []):
                    if isinstance(rr, dict):
                        wsite = rr.get('website') or ''
                        wuser = rr.get('website_user') or ''
                        wpass = rr.get('website_pass') or ''
                    else:
                        wsite = rr[0] or ''
                        wuser = rr[1] or ''
                        wpass = rr[2] or ''
                    if wsite or wuser or wpass:
                        websites.append({'website': str(wsite), 'user': str(wuser), 'pass': str(wpass)})
        except Exception:
            websites = []
        deadline_text = '-'
        try:
            sql_dead = """
                SELECT wn.deadline
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                LEFT JOIN data_website_niche wn ON wn.subdomain_id = s.subdomain_id
                WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL AND wn.deadline IS NOT NULL
                ORDER BY wn.deadline ASC
                LIMIT 1
            """
            if db.execute_query(sql_dead, (partner_id,)):
                rr = db.cur_hris.fetchone() or {}
                try:
                    dd = rr.get('deadline')
                except AttributeError:
                    dd = rr[0] if isinstance(rr, (list, tuple)) and len(rr) > 0 else None
                if dd:
                    try:
                        deadline_text = dd.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        deadline_text = str(dd)
        except Exception:
            deadline_text = '-'
        dom_text = ", ".join(domains) if domains else "-"
        login_lines = []
        for w in websites:
            line = f"{w.get('website','')}"
            if w.get('user'):
                line += f", user:  {w.get('user')}"
            if w.get('pass'):
                line += f", pass: {w.get('pass')}"
            login_lines.append(line.strip())
        login_text = "<br>".join(login_lines) if login_lines else "-"
        subject = "Task untuk update artikel"
        body = f"Task untuk update artikel: {dom_text}<br>Login URL: <br>{login_text}<br>Keyword Link: <a href=\"\">Keyword</a><br>Deadline: {deadline_text}"
        if notify_email and recipients:
            try:
                send_mail_notification(
                    list(dict.fromkeys([v for v in recipients if v])),
                    subject,
                    body
                )
            except Exception:
                pass
        if notify_whatsapp and recipients:
            try:
                send_whatsapp_notification(list(dict.fromkeys([v for v in recipients if v])), body)
            except Exception:
                pass
        return JsonResponse({'status': True, 'process_id': process_id})

class PublisherIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   pr.process_id,
                   s.subdomain_id,
                   CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   MAX(n.niche) AS niche,
                   (
                     SELECT COUNT(DISTINCT web_niche_id)
                     FROM data_website_niche w2
                     WHERE w2.subdomain_id = MAX(s.subdomain_id)
                   ) AS keyword_total,
                   (
                     SELECT COUNT(DISTINCT web_niche_id)
                     FROM data_website_niche w2
                     WHERE w2.subdomain_id = MAX(s.subdomain_id) AND w2.status = 'draft'
                   ) AS draft_total,
                   (
                     SELECT COUNT(DISTINCT web_niche_id)
                     FROM data_website_niche w2
                     WHERE w2.subdomain_id = MAX(s.subdomain_id) AND w2.status = 'posted'
                   ) AS posted_total,
                   MAX(dw.article_deadline) AS article_deadline
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website dw ON s.website_id = dw.website_id
            LEFT JOIN data_website_niche wn ON wn.subdomain_id = s.subdomain_id
            LEFT JOIN data_niche n ON n.niche_id = wn.niche_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
              AND s.subdomain_id IS NOT NULL
            GROUP BY mp.partner_id, pr.process_id, s.subdomain_id, s.subdomain, d.domain
            ORDER BY d.domain ASC, COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql, ('1002', '1002')):
            rows = db.cur_hris.fetchall() or []
        groups_by_partner = {}
        for r in rows:
            pid = None
            proc = None
            try:
                pid = r.get('partner_id')
                proc = r.get('process_id')
            except AttributeError:
                try:
                    pid = r[0]
                    proc = r[1]
                except Exception:
                    pid = None
                    proc = None
            if not pid:
                continue
            g = groups_by_partner.get(pid)
            if not g:
                g = {'partner_id': pid, 'process_id': proc, 'rows': []}
                groups_by_partner[pid] = g
            g['rows'].append(r)
        groups = []
        cum = 0
        for pid, g in groups_by_partner.items():
            g['rowspan'] = len(g['rows'])
            g['offset'] = cum
            groups.append(g)
            cum += len(g['rows'])
        niches = []
        q_niche = """
            SELECT niche_id, niche
            FROM data_niche
            ORDER BY niche ASC
        """
        if db.execute_query(q_niche):
            niches = db.cur_hris.fetchall() or []
        prompts = []
        q_prompts = """
            SELECT prompt_id, prompt
            FROM data_prompts
            ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC, prompt_id ASC
        """
        if db.execute_query(q_prompts):
            prompts = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'groups': groups,
            'niches': niches,
            'prompts': prompts,
        }
        return render(request, 'task/publisher/index.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class PublisherKeywordsSaveView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        niche_id_raw = (request.POST.get('niche_id') or '').strip()
        action = (request.POST.get('action') or '').strip().lower()
        status_list = request.POST.getlist('status[]') or []
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        try:
            niche_id = int(niche_id_raw) if niche_id_raw else None
        except Exception:
            niche_id = None
        keywords = request.POST.getlist('keyword[]') or request.POST.getlist('keyword') or []
        prompt_ids_raw = request.POST.getlist('prompt_id[]') or request.POST.getlist('prompt_id') or []
        prompt_ids = []
        for v in prompt_ids_raw:
            try:
                prompt_ids.append(int(v))
            except Exception:
                prompt_ids.append(None)
        
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        if not niche_id:
            return JsonResponse({'status': False, 'message': 'Niche wajib dipilih.'}, status=400)
        if not keywords:
            return JsonResponse({'status': False, 'message': 'Keyword wajib diisi.'}, status=400)
        domain_id = None
        if db.execute_query("SELECT domain_id FROM data_subdomain WHERE subdomain_id = %s LIMIT 1", (subdomain_id,)):
            rr = db.cur_hris.fetchone() or {}
            try:
                domain_id = rr.get('domain_id')
            except AttributeError:
                try:
                    domain_id = rr[0]
                except Exception:
                    domain_id = None
        prompt_map = {}
        if prompt_ids:
            placeholders = ",".join(["%s"] * len([pid for pid in prompt_ids if pid]))
            if placeholders:
                sqlp = f"SELECT prompt_id, prompt FROM data_prompts WHERE prompt_id IN ({placeholders})"
                if db.execute_query(sqlp, tuple([pid for pid in prompt_ids if pid])):
                    for row in db.cur_hris.fetchall() or []:
                        pid = row.get('prompt_id') if isinstance(row, dict) else row[0]
                        txt = row.get('prompt') if isinstance(row, dict) else row[1]
                        prompt_map[pid] = txt
        mdb = str(admin.get('user_id', ''))[:36]
        mdb_name = admin.get('user_alias', '')
        inserted = 0
        updated = 0
        final_keywords = [(str(k or '').strip()) for k in keywords if (str(k or '').strip())]
        dup_payload = []
        seen_kw = set()
        for fk in final_keywords:
            if fk in seen_kw:
                dup_payload.append(fk)
            else:
                seen_kw.add(fk)
        dup_db = []
        if final_keywords and action != 'update':
            uniq_list = list(set(final_keywords))
            placeholders = ",".join(["%s"] * len(uniq_list))
            sql_dup = f"SELECT keyword, COUNT(*) AS cnt FROM data_website_niche WHERE subdomain_id=%s AND niche_id=%s AND keyword IN ({placeholders}) GROUP BY keyword"
            params_dup = tuple([subdomain_id, niche_id] + uniq_list)
            if db.execute_query(sql_dup, params_dup):
                rows_dup = db.cur_hris.fetchall() or []
                for rd in rows_dup:
                    try:
                        kwd = rd.get('keyword') if isinstance(rd, dict) else rd[0]
                        cntv = rd.get('cnt') if isinstance(rd, dict) else rd[1]
                    except Exception:
                        kwd = None
                        cntv = 0
                    if not kwd:
                        continue
                    if cntv and cntv >= 1:
                        dup_db.append(kwd)
        dup_all = list(set(dup_payload + dup_db))
        if dup_all:
            msg = 'Keyword duplikat untuk subdomain dan niche: ' + ", ".join(dup_all)
            return JsonResponse({'status': False, 'message': msg, 'duplicates': dup_all}, status=400)
        if action == 'update':
            db.execute_query(
                "DELETE FROM data_website_niche WHERE subdomain_id=%s AND niche_id=%s",
                (subdomain_id, niche_id)
            )
        for idx, kw in enumerate(keywords):
            kw_text = (kw or '').strip()
            if not kw_text:
                continue
            pid = prompt_ids[idx] if idx < len(prompt_ids) else None
            ptxt = prompt_map.get(pid) if pid else None
            if ptxt:
                try:
                    ptxt = ptxt.replace('{keyword}', kw_text)
                except Exception:
                    pass
            sv = status_list[idx] if idx < len(status_list) and (status_list[idx] or '').strip() else 'posted'
            ok = db.execute_query(
                """
                INSERT INTO data_website_niche
                (domain_id, subdomain_id, niche_id, keyword, prompt, status, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (domain_id, subdomain_id, niche_id, kw_text, ptxt, sv, mdb, mdb_name)
            )
            if ok:
                if action == 'update':
                    updated += 1
                else:
                    inserted += 1
        db.commit()
        return JsonResponse({'status': True, 'inserted': inserted, 'updated': updated})

class PublisherKeywordsLoadView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.GET.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        sql = """
            SELECT k.keyword,
                   k.status,
                   k.niche_id,
                   k.prompt AS prompt_text
            FROM data_website_niche k
            WHERE k.subdomain_id = %s
            ORDER BY COALESCE(k.mdd, '0000-00-00 00:00:00') DESC
        """
        items = []
        niche_id = None
        if db.execute_query(sql, (subdomain_id,)):
            rows = db.cur_hris.fetchall() or []
            # Load all prompts to attempt reverse-matching
            prompts = []
            if db.execute_query("SELECT prompt_id, prompt FROM data_prompts"):
                prompts = db.cur_hris.fetchall() or []
            for r in rows:
                kw = r.get('keyword') if isinstance(r, dict) else r[0]
                st = r.get('status') if isinstance(r, dict) else r[1]
                nid = r.get('niche_id') if isinstance(r, dict) else r[2]
                ptxt = r.get('prompt_text') if isinstance(r, dict) else r[3]
                if not niche_id and nid:
                    niche_id = nid
                matched_pid = None
                # Try to find the template whose replacement equals stored prompt text
                for pr in prompts:
                    try:
                        pid = pr.get('prompt_id') if isinstance(pr, dict) else pr[0]
                        base = pr.get('prompt') if isinstance(pr, dict) else pr[1]
                    except Exception:
                        pid = None
                        base = None
                    if not base:
                        continue
                    candidate = None
                    try:
                        candidate = str(base).replace('{keyword}', str(kw or ''))
                    except Exception:
                        candidate = base
                    if str(candidate or '').strip() == str(ptxt or '').strip():
                        matched_pid = pid
                        break
                items.append({'keyword': kw, 'status': st, 'niche_id': nid, 'prompt_id': matched_pid, 'prompt_text': ptxt})
        return JsonResponse({'status': True, 'niche_id': niche_id, 'items': items})

class PublisherKeywordsByNicheView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        niche_id_raw = (request.GET.get('niche_id') or '').strip()
        try:
            niche_id = int(niche_id_raw) if niche_id_raw else None
        except Exception:
            niche_id = None
        if not niche_id:
            return JsonResponse({'status': True, 'items': []})
        sql = """
            SELECT keyword_id, keyword
            FROM data_keywords
            WHERE niche_id = %s
            ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC, keyword_id ASC
        """
        items = []
        if db.execute_query(sql, (niche_id,)):
            for r in (db.cur_hris.fetchall() or []):
                try:
                    kid = r.get('keyword_id')
                    kw = r.get('keyword')
                except AttributeError:
                    try:
                        kid = r[0]
                        kw = r[1]
                    except Exception:
                        kid = None
                        kw = None
                items.append({'keyword_id': kid, 'keyword': kw})
        return JsonResponse({'status': True, 'items': items})

@method_decorator(csrf_exempt, name='dispatch')
class PublisherKeywordsDeleteView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw) if subdomain_id_raw else None
        except Exception:
            subdomain_id = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        ok = db.execute_query(
            "DELETE FROM data_website_niche WHERE subdomain_id=%s",
            (subdomain_id,)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal menghapus data.'}, status=500)
        db.commit()
        return JsonResponse({'status': True, 'deleted': True})

@method_decorator(csrf_exempt, name='dispatch')
class PublisherSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        recipients = request.POST.getlist('recipients[]') or request.POST.getlist('recipients') or []
        notify_email_raw = (request.POST.get('notify_email') or '').strip().lower()
        notify_whatsapp_raw = (request.POST.get('notify_whatsapp') or '').strip().lower()
        notify_email = notify_email_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        notify_whatsapp = notify_whatsapp_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        # cek proses saat ini
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        # validasi keywords: semua subdomain partner harus punya minimal 1 keyword
        sql_chk = """
            SELECT s.subdomain_id,
                   CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   COUNT(k.keyword) AS kw_count
            FROM data_media_partner_domain pd
            JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website_niche k ON k.subdomain_id = s.subdomain_id
            WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL
            GROUP BY s.subdomain_id, s.subdomain, d.domain
        """
        missing = []
        if db.execute_query(sql_chk, (partner_id,)):
            for rr in (db.cur_hris.fetchall() or []):
                nm = rr.get('subdomain_name') if isinstance(rr, dict) else rr[1]
                cnt = rr.get('kw_count') if isinstance(rr, dict) else rr[2]
                if (cnt or 0) <= 0 and nm:
                    missing.append(nm)
        if missing:
            return JsonResponse({'status': False, 'message': 'Keyword belum diisi untuk subdomain', 'missing': missing}, status=400)
        # update proses saat ini menjadi selesai
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb_finish=%s, mdb_finish_name=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', admin_id, admin_alias, process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        # buat proses berikutnya (flow 1003)
        from datetime import datetime
        import random
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        process_id = f"PR{base}{rnd}"[:20]
        sql_proc = """
            INSERT INTO data_media_process
            (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        catatan_text = ", ".join([v for v in recipients if v]) if recipients else ""
        catatan_val = f"Assigned to: {catatan_text}" if catatan_text else None
        params_proc = (
            process_id,
            partner_id,
            '1003',
            None,
            'waiting',
            'process',
            catatan_val,
            admin_id,
            admin_alias,
        )
        if not db.execute_query(sql_proc, params_proc):
            return JsonResponse({'status': False, 'message': 'Gagal mengirim data ke proses berikutnya.'}, status=500)
        db.commit()
        subdomains = []
        try:
            sql_sub = """
                SELECT DISTINCT CONCAT(s.subdomain, '.', d.domain) AS website_name
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL
            """
            if db.execute_query(sql_sub, (partner_id,)):
                for rr in (db.cur_hris.fetchall() or []):
                    try:
                        nm = rr.get('website_name')
                    except AttributeError:
                        nm = rr[0]
                    if nm:
                        subdomains.append(str(nm))
        except Exception:
            subdomains = []
        sub_text = "<br>".join(subdomains) if subdomains else "-"
        subject = "Task untuk setting cloacking"
        body = f"Task untuk setting cloacking: <br>{sub_text},<br><br>Tolong setting untuk cloacking dan assign ke Ads Team."

        if notify_email and recipients:
            try:
                send_mail_notification(
                    list(dict.fromkeys([v for v in recipients if v])),
                    subject,
                    body
                )
            except Exception:
                pass
        if notify_whatsapp and recipients:
            try:
                send_whatsapp_notification(list(dict.fromkeys([v for v in recipients if v])), body)
            except Exception:
                pass
        return JsonResponse({'status': True, 'process_id': process_id})

class TrackerIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   pr.process_id,
                   s.subdomain_id,
                   CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   s.tracker,
                   s.tracker_params,
                   s.plugin_lp,
                   s.plugin_params
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
              AND s.subdomain_id IS NOT NULL
            GROUP BY mp.partner_id, pr.process_id, s.subdomain_id, s.subdomain, d.domain, s.tracker, s.tracker_params, s.plugin_lp, s.plugin_params
            ORDER BY d.domain ASC, COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql, ('1004', '1004')):
            rows = db.cur_hris.fetchall() or []
        groups_by_partner = {}
        for r in rows:
            pid = None
            proc = None
            try:
                pid = r.get('partner_id')
                proc = r.get('process_id')
            except AttributeError:
                try:
                    pid = r[0]
                    proc = r[1]
                except Exception:
                    pid = None
                    proc = None
            if not pid:
                continue
            g = groups_by_partner.get(pid)
            if not g:
                g = {'partner_id': pid, 'process_id': proc, 'rows': []}
                groups_by_partner[pid] = g
            g['rows'].append(r)
        groups = []
        for pid, g in groups_by_partner.items():
            g['rowspan'] = len(g['rows'])
            groups.append(g)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'groups': groups,
        }
        return render(request, 'task/tracker/index.html', context)

class SelesaiIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT
                w.website_id AS website_id,
                s.*, MAX(dmp.pic) AS pic, MAX(dmp.partner_name) AS partner_name, MAX(dmp.request_date) AS request_date, MAX(dmp.partner_contact) AS partner_contact,
                MAX(dmp.partner_id) AS partner_id,
                CONCAT(s.subdomain, '.', d.domain) AS website_name,
                s.public_ipv4, MAX(ds.hostname) AS hostname, MAX(ds.provider) AS provider, 
                MAX(ds.vcpu_count) AS vcpu, MAX(ds.memory_gb) AS memory_gb,
                w.website_user AS website_user,
                w.website_pass AS website_pass,
                (SELECT COUNT(*) FROM data_website_niche k WHERE k.subdomain_id = s.subdomain_id) AS keyword_count,
                MAX(a.account_name) AS fb_account,
                MAX(b.fanpage) AS fb_fanpage,
                MAX(b.daily_budget) AS fb_daily_budget,
                CONCAT_WS(', ',
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn
                        WHERE FIND_IN_SET(mn.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn.tier AS CHAR), 'Tier ', '') = '1'
                    ) THEN 'Tier 1' END,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM master_negara mn2
                        WHERE FIND_IN_SET(mn2.negara_nm, MAX(b.country))
                          AND REPLACE(CAST(mn2.tier AS CHAR), 'Tier ', '') = '2'
                    ) THEN 'Tier 2' END
                ) AS country_tier,
                MAX(n.niche) AS niche
            FROM data_website w
            INNER JOIN data_subdomain s ON s.website_id = w.website_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            INNER JOIN data_media_partner_domain dmpd ON d.domain_id = dmpd.domain_id
            INNER JOIN data_media_partner dmp ON dmpd.partner_id = dmp.partner_id
            LEFT JOIN data_website_niche wn ON wn.subdomain_id = s.subdomain_id
            LEFT JOIN data_niche n ON n.niche_id = wn.niche_id
            LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
            LEFT JOIN master_account_ads a ON a.account_ads_id = COALESCE(NULLIF(b.account_ads_id_2, ''), b.account_ads_id_1)
            LEFT JOIN data_servers ds ON s.public_ipv4 = ds.public_ipv4
            WHERE s.subdomain_id IS NOT NULL AND dmp.status = 'completed'
            GROUP BY s.subdomain_id
            ORDER BY s.subdomain ASC, COALESCE(w.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql):
            rows = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'websites': rows,
        }
        return render(request, 'task/selesai/index.html', context)

class SelesaiDetailView(View):
    def get(self, request, partner_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        partner = None
        sql_partner = """
            SELECT partner_id, partner_name, partner_contact, partner_region, request_date, pic
            FROM data_media_partner
            WHERE partner_id = %s
            LIMIT 1
        """
        if db.execute_query(sql_partner, (partner_id,)):
            partner = db.cur_hris.fetchone()
        domains = []
        sql_domains = """
            SELECT
                d.domain_id,
                d.domain,
                ds.hostname,
                ds.label,
                COALESCE(prv1.provider, d.provider) AS provider_name,
                COALESCE(prv2.provider, d.registrar) AS registrar_name,
                w.domain AS w_domain,
                w.website,
                w.website_user,
                w.website_pass,
                w.article_status,
                w.article_deadline
            FROM data_media_partner_domain pd
            JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_servers ds ON ds.server_id = d.server_id
            LEFT JOIN data_website w ON w.website_id = d.website_id
            LEFT JOIN data_server_registrar_provider prv1 ON prv1.provider = d.provider
            LEFT JOIN data_server_registrar_provider prv2 ON prv2.provider = d.registrar
            WHERE pd.partner_id = %s
            ORDER BY d.domain ASC
        """
        if db.execute_query(sql_domains, (partner_id,)):
            domains = db.cur_hris.fetchall() or []
        subdomains = []
        sql_subdomains = """
            SELECT
                s.subdomain_id,
                s.subdomain,
                d.domain AS domain,
                s.cloudflare,
                s.public_ipv4,
                ds.hostname,
                ds.label,
                w.domain AS w_domain,
                w.website,
                w.website_user,
                w.website_pass,
                w.article_status,
                w.article_deadline,
                s.tracker,
                s.tracker_params,
                s.plugin_setup,
                s.plugin_lp,
                s.plugin_params
            FROM data_media_partner_domain pd
            JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_website w ON w.website_id = s.website_id
            LEFT JOIN data_servers ds ON ds.public_ipv4 = s.public_ipv4
            WHERE pd.partner_id = %s
            ORDER BY s.subdomain ASC
        """
        if db.execute_query(sql_subdomains, (partner_id,)):
            subdomains = db.cur_hris.fetchall() or []
        fb_ads = []
        sql_fb_ads = """
            SELECT
                f.ads_id,
                f.domain_id,
                f.subdomain_id,
                COALESCE(ma1.account_name, ma2.account_name) AS account_name,
                f.fanpage,
                f.interest,
                f.country,
                f.daily_budget,
                f.status,
                d.domain AS domain,
                CONCAT(s.subdomain, '.', d2.domain) AS subdomain_name
            FROM data_media_fb_ads f
            LEFT JOIN data_domains d ON d.domain_id = f.domain_id
            LEFT JOIN data_subdomain s ON s.subdomain_id = f.subdomain_id
            LEFT JOIN data_domains d2 ON d2.domain_id = s.domain_id
            LEFT JOIN master_account_ads ma1 ON ma1.account_ads_id = f.account_ads_id_1
            LEFT JOIN master_account_ads ma2 ON ma2.account_ads_id = f.account_ads_id_2
            WHERE f.domain_id IN (
                SELECT domain_id FROM data_media_partner_domain WHERE partner_id = %s
            )
            OR f.subdomain_id IN (
                SELECT s2.subdomain_id
                FROM data_subdomain s2
                JOIN data_media_partner_domain pd2 ON pd2.domain_id = s2.domain_id
                WHERE pd2.partner_id = %s
            )
            ORDER BY COALESCE(f.mdd, '0000-00-00 00:00:00') DESC
        """
        if db.execute_query(sql_fb_ads, (partner_id, partner_id)):
            fb_ads = db.cur_hris.fetchall() or []
        processes = []
        sql_processes = """
            SELECT
                p.flow_id,
                df.task_name,
                p.process_st,
                p.action_st,
                p.catatan,
                p.mdb_name,
                p.mdd,
                p.mdb_finish_name,
                p.mdd_finish
            FROM data_media_process p
            LEFT JOIN data_flow df ON df.flow_id = p.flow_id
            WHERE p.partner_id = %s
            ORDER BY COALESCE(p.mdd, '0000-00-00 00:00:00') DESC
        """
        if db.execute_query(sql_processes, (partner_id,)):
            processes = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'partner': partner,
            'domains': domains,
            'subdomains': subdomains,
            'fb_ads': fb_ads,
            'processes': processes,
        }
        return render(request, 'task/selesai/detail.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class TrackerUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        tracker = (request.POST.get('tracker') or '').strip()
        tracker_params = (request.POST.get('tracker_params') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        ok = db.execute_query(
            """
            UPDATE data_subdomain SET tracker=%s, tracker_params=%s, mdd=NOW()
            WHERE subdomain_id=%s
            """,
            (tracker, tracker_params, subdomain_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui tracker.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class TrackerSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        recipients = request.POST.getlist('recipients[]') or request.POST.getlist('recipients') or []
        notify_email_raw = (request.POST.get('notify_email') or '').strip().lower()
        notify_whatsapp_raw = (request.POST.get('notify_whatsapp') or '').strip().lower()
        notify_email = notify_email_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        notify_whatsapp = notify_whatsapp_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb_finish=%s, mdb_finish_name=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', admin_id, admin_alias, process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        from datetime import datetime
        import random
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        process_id = f"PR{base}{rnd}"[:20]
        sql_proc = """
            INSERT INTO data_media_process
            (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        catatan_text = ", ".join([v for v in recipients if v]) if recipients else ""
        catatan_val = f"Assigned to: {catatan_text}" if catatan_text else None
        params_proc = (
            process_id,
            partner_id,
            '1005',
            None,
            'waiting',
            'process',
            catatan_val,
            admin_id,
            admin_alias,
        )
        if not db.execute_query(sql_proc, params_proc):
            return JsonResponse({'status': False, 'message': 'Gagal mengirim data ke proses berikutnya.'}, status=500)
        db.commit()
        subdomains = []
        trackers = []
        params_list = []
        try:
            sql_info = """
                SELECT CONCAT(s.subdomain, '.', d.domain) AS website_name, s.tracker, s.tracker_params
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                WHERE pd.partner_id = %s AND s.subdomain_id IS NOT NULL
            """
            if db.execute_query(sql_info, (partner_id,)):
                for rr in (db.cur_hris.fetchall() or []):
                    if isinstance(rr, dict):
                        nm = rr.get('website_name')
                        tr = rr.get('tracker')
                        tp = rr.get('tracker_params')
                    else:
                        nm = rr[0]
                        tr = rr[1]
                        tp = rr[2]
                    if nm:
                        subdomains.append(str(nm))
                    if tr:
                        trackers.append(str(tr))
                    if tp:
                        params_list.append(str(tp))
        except Exception:
            pass
        subs_text = "<br>".join(subdomains) if subdomains else "-"
        trackers_text = "<br>".join(trackers) if trackers else "-"
        params_text = "<br>".join(params_list) if params_list else "-"
        subject = "Task iklan untuk domain"
        body = f"Task iklan untuk domain:<br>{subs_text}<br><br>Domain berikut sudah siap untuk diiklankan, silahkan diproses.<br>URL Iklan: <br>{trackers_text}<br>Parameter: <br>{params_text}"
        if notify_email and recipients:
            try:
                send_mail_notification(
                    list(dict.fromkeys([v for v in recipients if v])),
                    subject,
                    body
                )
            except Exception:
                pass
        if notify_whatsapp and recipients:
            try:
                send_whatsapp_notification(list(dict.fromkeys([v for v in recipients if v])), body)
            except Exception:
                pass
        return JsonResponse({'status': True, 'process_id': process_id})

class PluginIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   pr.process_id,
                   s.subdomain_id,
                   CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   s.plugin_setup,
                   s.plugin_lp,
                   s.plugin_params
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
              AND s.subdomain_id IS NOT NULL
            GROUP BY mp.partner_id, pr.process_id, s.subdomain_id, s.subdomain, d.domain, s.plugin_setup, s.plugin_lp, s.plugin_params
            ORDER BY d.domain ASC, COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
        """
        rows = []
        if db.execute_query(sql, ('1003', '1003')):
            rows = db.cur_hris.fetchall() or []
        groups_by_partner = {}
        for r in rows:
            pid = None
            proc = None
            try:
                pid = r.get('partner_id')
                proc = r.get('process_id')
            except AttributeError:
                try:
                    pid = r[0]
                    proc = r[1]
                except Exception:
                    pid = None
                    proc = None
            if not pid:
                continue
            g = groups_by_partner.get(pid)
            if not g:
                g = {'partner_id': pid, 'process_id': proc, 'rows': []}
                groups_by_partner[pid] = g
            g['rows'].append(r)
        groups = []
        for pid, g in groups_by_partner.items():
            g['rowspan'] = len(g['rows'])
            groups.append(g)
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'groups': groups,
        }
        return render(request, 'task/plugin/index.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class PluginUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        plugin_setup = (request.POST.get('plugin_setup') or '').strip()
        plugin_lp = (request.POST.get('plugin_lp') or '').strip()
        plugin_params = (request.POST.get('plugin_params') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        ok = db.execute_query(
            """
            UPDATE data_subdomain SET plugin_setup=%s, plugin_lp=%s, plugin_params=%s, mdd=NOW()
            WHERE subdomain_id=%s
            """,
            (plugin_setup, plugin_lp, plugin_params, subdomain_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui plugin.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class PluginSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb_finish=%s, mdb_finish_name=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', admin_id, admin_alias, process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        from datetime import datetime
        import random
        now = datetime.now()
        base = now.strftime('%Y%m%d%H%M%S')
        rnd = f"{random.randint(0, 9999):04d}"
        process_id = f"PR{base}{rnd}"[:20]
        sql_proc = """
            INSERT INTO data_media_process
            (process_id, partner_id, flow_id, flow_revisi_id, process_st, action_st, catatan, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        params_proc = (
            process_id,
            partner_id,
            '1004',
            None,
            'waiting',
            'process',
            None,
            admin_id,
            admin_alias,
        )
        if not db.execute_query(sql_proc, params_proc):
            return JsonResponse({'status': False, 'message': 'Gagal mengirim data ke proses berikutnya.'}, status=500)
        db.commit()
        return JsonResponse({'status': True, 'process_id': process_id})

class AdsIndexView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        sql = """
            SELECT mp.partner_id,
                   pr.process_id,
                   s.subdomain_id,
                   CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   b.account_ads_id_1,
                   b.fanpage,
                   b.interest,
                   b.country,
                   b.daily_budget,
                   s.tracker,
                   s.tracker_params,
                   MAX(dn.niche) AS niche,
                   COUNT(dwn.web_niche_id) AS keyword_total
            FROM data_media_partner mp
            INNER JOIN (
                SELECT p.process_id, p.partner_id, p.flow_id, COALESCE(p.mdd, '0000-00-00 00:00:00') AS mdd
                FROM data_media_process p
                WHERE p.flow_id = %s AND p.process_st = 'waiting'
            ) pr ON pr.partner_id = mp.partner_id
            INNER JOIN data_flow df ON df.flow_id = pr.flow_id
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
            LEFT JOIN data_website_niche dwn ON s.subdomain_id = dwn.subdomain_id
            LEFT JOIN data_niche dn ON dwn.niche_id = dn.niche_id
            WHERE mp.status = 'waiting'
              AND pr.mdd = (
                  SELECT MAX(COALESCE(mdd, '0000-00-00 00:00:00'))
                  FROM data_media_process
                  WHERE partner_id = mp.partner_id AND flow_id = %s AND process_st = 'waiting'
              )
              AND s.subdomain_id IS NOT NULL
            GROUP BY mp.partner_id, pr.process_id, s.subdomain_id, s.subdomain, d.domain, b.account_ads_id_1, b.fanpage, b.interest, b.country, b.daily_budget, s.tracker, s.tracker_params
            ORDER BY d.domain ASC
        """
        rows = []
        if db.execute_query(sql, ('1005', '1005')):
            rows = db.cur_hris.fetchall() or []
        groups_by_partner = {}
        for r in rows:
            pid = None
            proc = None
            try:
                pid = r.get('partner_id')
                proc = r.get('process_id')
            except AttributeError:
                try:
                    pid = r[0]
                    proc = r[1]
                except Exception:
                    pid = None
                    proc = None
            if not pid:
                continue
            g = groups_by_partner.get(pid)
            if not g:
                g = {'partner_id': pid, 'process_id': proc, 'rows': []}
                groups_by_partner[pid] = g
            g['rows'].append(r)
        groups = []
        cum = 0
        for pid, g in groups_by_partner.items():
            g['rowspan'] = len(g['rows'])
            g['offset'] = cum
            groups.append(g)
            cum += len(g['rows'])
        accounts = []
        if db.execute_query("SELECT account_ads_id, account_name FROM master_account_ads ORDER BY account_name ASC"):
            accounts = db.cur_hris.fetchall() or []
        countries = []
        if db.execute_query("SELECT negara_kd, negara_nm, tier FROM master_negara ORDER BY negara_nm ASC"):
            countries = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'groups': groups,
            'accounts': accounts,
            'countries': countries,
        }
        return render(request, 'task/ads/index.html', context)

class AdsPagesView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        account_ads_id = (request.GET.get('account_ads_id') or request.POST.get('account_ads_id') or '').strip()
        if not account_ads_id:
            return JsonResponse({'status': False, 'message': 'Account wajib diisi', 'pages': []}, status=400)
        token = None
        acc_id = None
        sql = """
            SELECT access_token, account_id
            FROM master_account_ads
            WHERE account_ads_id = %s
            LIMIT 1
        """
        if db.execute_query(sql, (account_ads_id,)):
            row = db.cur_hris.fetchone() or {}
            try:
                token = row.get('access_token')
                acc_id = row.get('account_id')
            except AttributeError:
                try:
                    token = row[0]
                    acc_id = row[1]
                except Exception:
                    token = None
                    acc_id = None
        if not token:
            return JsonResponse({'status': False, 'message': 'Token tidak ditemukan', 'pages': []}, status=404)
        try:
            url = 'https://graph.facebook.com/v17.0/me/accounts'
            params = {'access_token': token, 'limit': 200}
            r = requests.get(url, params=params, timeout=15)
            if r.status_code != 200:
                return JsonResponse({'status': False, 'message': 'Gagal memuat fanpage', 'pages': []}, status=502)
            data = {}
            try:
                data = r.json() or {}
            except Exception:
                data = {}
            items = data.get('data') or []
            pages = []
            for p in items:
                pid = str(p.get('id') or '')
                nm = str(p.get('name') or pid)
                if pid:
                    pages.append({'id': pid, 'name': nm})
            return JsonResponse({'status': True, 'pages': pages})
        except Exception:
            return JsonResponse({'status': False, 'message': 'Terjadi kesalahan memuat fanpage', 'pages': []}, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class AdsUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        account_ads_id_1 = (request.POST.get('account_ads_id_1') or '').strip()
        fanpage = (request.POST.get('fanpage') or '').strip()
        interest = (request.POST.get('interest') or '').strip()
        country_vals = request.POST.getlist('country[]') or request.POST.getlist('country') or []
        daily_budget_raw = (request.POST.get('daily_budget') or '').strip()
        mdb = str(admin.get('user_id', ''))[:36]
        mdb_name = admin.get('user_alias', '')
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        try:
            daily_budget = float(daily_budget_raw) if daily_budget_raw else None
        except Exception:
            daily_budget = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        countries_text = None
        if country_vals:
            items = [str(v).strip() for v in country_vals if str(v).strip()]
            countries_text = ",".join(items) if items else None
        dup_name = None
        if fanpage:
            sql_dup = """
            SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name
            FROM data_media_fb_ads b
            INNER JOIN data_subdomain s ON s.subdomain_id = b.subdomain_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            WHERE b.fanpage = %s AND b.subdomain_id <> %s AND b.status = 'on'
            LIMIT 1
            """
            if db.execute_query(sql_dup, (fanpage, subdomain_id)):
                rr = db.cur_hris.fetchone() or {}
                try:
                    dup_name = rr.get('subdomain_name')
                except AttributeError:
                    try:
                        dup_name = rr[0]
                    except Exception:
                        dup_name = None
        if dup_name:
            return JsonResponse({'status': False, 'message': 'Fanpage telah digunakan untuk subdomain ' + str(dup_name or '')}, status=400)
        exists = False
        if db.execute_query("SELECT ads_id FROM data_media_fb_ads WHERE subdomain_id=%s LIMIT 1", (subdomain_id,)):
            row = db.cur_hris.fetchone() or {}
            try:
                exists = bool(row.get('ads_id'))
            except AttributeError:
                try:
                    exists = row[0] is not None
                except Exception:
                    exists = False
        ok = False
        if exists:
            ok = db.execute_query(
                """
                UPDATE data_media_fb_ads
                SET account_ads_id_1=%s, fanpage=%s, interest=%s, country=%s, daily_budget=%s, mdb=%s, mdb_name=%s, mdd=NOW()
                WHERE subdomain_id=%s
                """,
                (account_ads_id_1 or None, fanpage or None, interest or None, countries_text, daily_budget, mdb, mdb_name, subdomain_id)
            )
        else:
            domain_id = None
            if db.execute_query("SELECT domain_id FROM data_subdomain WHERE subdomain_id=%s LIMIT 1", (subdomain_id,)):
                rr = db.cur_hris.fetchone() or {}
                try:
                    domain_id = rr.get('domain_id')
                except AttributeError:
                    try:
                        domain_id = rr[0]
                    except Exception:
                        domain_id = None
            ok = db.execute_query(
                """
                INSERT INTO data_media_fb_ads
                (domain_id, subdomain_id, account_ads_id_1, fanpage, interest, country, daily_budget, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (domain_id, subdomain_id, account_ads_id_1 or None, fanpage or None, interest or None, countries_text, daily_budget, mdb, mdb_name)
            )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui Ads.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class AdsSendView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        partner_id = (request.POST.get('partner_id') or '').strip()
        process_id_cur = (request.POST.get('process_id') or '').strip()
        recipients = request.POST.getlist('recipients[]') or request.POST.getlist('recipients') or []
        notify_email_raw = (request.POST.get('notify_email') or '').strip().lower()
        notify_email = notify_email_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        notify_whatsapp_raw = (request.POST.get('notify_whatsapp') or '').strip().lower()
        notify_whatsapp = notify_whatsapp_raw in ['1', 'true', 'yes', 'on', 'checked', 'y']
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not process_id_cur:
            return JsonResponse({'status': False, 'message': 'Process ID wajib diisi.'}, status=400)
        if not db.execute_query("SELECT process_id FROM data_media_process WHERE process_id = %s LIMIT 1", (process_id_cur,)):
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak ditemukan.'}, status=404)
        row = db.cur_hris.fetchone() or {}
        try:
            existing_pid = row.get('process_id')
        except AttributeError:
            try:
                existing_pid = row[0]
            except Exception:
                existing_pid = None
        if not existing_pid:
            return JsonResponse({'status': False, 'message': 'Proses saat ini tidak valid.'}, status=400)
        admin_id = str(admin.get('user_id', ''))[:10]
        admin_alias = admin.get('user_alias', '')
        # Validate required fields for all subdomains under this partner
        sql_chk = """
            SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                   b.account_ads_id_1,
                   b.interest,
                   b.country,
                   b.daily_budget
            FROM data_media_partner mp
            INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
            INNER JOIN data_domains d ON d.domain_id = pd.domain_id
            LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
            LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
            WHERE mp.partner_id = %s AND s.subdomain_id IS NOT NULL
            ORDER BY d.domain ASC
        """
        problems = []
        if db.execute_query(sql_chk, (partner_id,)):
            for r in (db.cur_hris.fetchall() or []):
                try:
                    nm = r.get('subdomain_name') if isinstance(r, dict) else r[0]
                    acc = r.get('account_ads_id_1') if isinstance(r, dict) else r[1]
                    intr = r.get('interest') if isinstance(r, dict) else r[2]
                    ctr = r.get('country') if isinstance(r, dict) else r[3]
                    bud = r.get('daily_budget') if isinstance(r, dict) else r[4]
                except Exception:
                    nm = None; acc = None; intr = None; ctr = None; bud = None
                missing = []
                if not (acc or '').strip(): missing.append('FB Account')
                if not (intr or '').strip(): missing.append('Interest')
                if not (ctr or '').strip(): missing.append('Countries')
                if bud is None or str(bud).strip() == '': missing.append('Daily Budget')
                if missing:
                    problems.append(f"{nm or '-'} ({', '.join(missing)})")
        if problems:
            return JsonResponse({'status': False, 'message': 'Lengkapi semua field sebelum kirim', 'missing': problems}, status=400)
        ok_upd = db.execute_query(
            """
            UPDATE data_media_process SET process_st=%s, action_st=%s, mdb_finish=%s, mdb_finish_name=%s, mdd_finish=NOW()
            WHERE process_id=%s
            """,
            ('approve', 'done', admin_id, admin_alias, process_id_cur)
        )
        if not ok_upd:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui proses saat ini.'}, status=500)
        db.commit()
        ok_partner = db.execute_query(
            """
            UPDATE data_media_partner SET status=%s, mdb=%s, mdb_name=%s, mdd=NOW()
            WHERE partner_id=%s
            """,
            ('completed', admin_id, admin_alias, partner_id)
        )
        if not ok_partner:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui status partner.'}, status=500)
        db.commit()
        try:
            def fmt_idr(v):
                try:
                    n = float(v)
                except Exception:
                    return str(v or '')
                s = '{:,.0f}'.format(n).replace(',', '.')
                return 'Rp ' + s
            sql_info = """
                SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name,
                       b.country,
                       b.daily_budget,
                       b.account_ads_id_1,
                       m.account_name,
                       b.fanpage
                FROM data_media_partner mp
                INNER JOIN data_media_partner_domain pd ON pd.partner_id = mp.partner_id
                INNER JOIN data_domains d ON d.domain_id = pd.domain_id
                LEFT JOIN data_subdomain s ON s.domain_id = d.domain_id
                LEFT JOIN data_media_fb_ads b ON b.subdomain_id = s.subdomain_id
                LEFT JOIN master_account_ads m ON m.account_ads_id = b.account_ads_id_1
                WHERE mp.partner_id = %s AND s.subdomain_id IS NOT NULL
                ORDER BY d.domain ASC
            """
            subdomains = []
            blocks = []
            if db.execute_query(sql_info, (partner_id,)):
                for r in (db.cur_hris.fetchall() or []):
                    try:
                        nm = r.get('subdomain_name') if isinstance(r, dict) else r[0]
                        ctr = r.get('country') if isinstance(r, dict) else r[1]
                        bud = r.get('daily_budget') if isinstance(r, dict) else r[2]
                        acc_name = r.get('account_name') if isinstance(r, dict) else (r[4] if len(r) > 4 else None)
                        fanpage = r.get('fanpage') if isinstance(r, dict) else (r[5] if len(r) > 5 else None)
                    except Exception:
                        nm = None; ctr = None; bud = None; acc_name = None; fanpage = None
                    if nm: subdomains.append(str(nm))
                    var_nm = str(nm or '-')
                    var_bud = fmt_idr(bud)
                    var_ctr = str(ctr or '')
                    var_acc = str(acc_name or '')
                    var_fp = str(fanpage or '')
                    blocks.append(
                        var_nm + "<br>" +
                        "Budget: " + var_bud + "<br>" +
                        "Target Negara: " + var_ctr + "<br>" +
                        "Akun FB: " + var_acc + "<br>" +
                        "Fanpage: " + var_fp
                    )
            subj_domains = ", ".join(subdomains) if subdomains else ""
            subject = ("Domain " + subj_domains + " iklan sudah ditayangkan") if subj_domains else "Iklan sudah ditayangkan"
            body = "Iklan sudah ditayangkan untuk domain berikut: <br>" + (("<br><br>".join(blocks)) if blocks else "-")
            if notify_email and recipients:
                try:
                    send_mail_notification(list(dict.fromkeys([v for v in recipients if v])), subject, body)
                except Exception:
                    pass
            if notify_whatsapp and recipients:
                try:
                    send_whatsapp_notification(list(dict.fromkeys([v for v in recipients if v])), body)
                except Exception:
                    pass
        except Exception:
            pass
        return JsonResponse({'status': True})

class ActiveAdsEditView(View):
    def get(self, request, subdomain_id):
        if 'hris_admin' not in request.session:
            return redirect('admin_login')
        admin = request.session.get('hris_admin', {})
        active_portal_id = request.session.get('active_portal_id', '12')
        db = data_mysql()
        subdomain_name = None
        sql_sd = """
            SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name
            FROM data_subdomain s
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            WHERE s.subdomain_id = %s
            LIMIT 1
        """
        if db.execute_query(sql_sd, (subdomain_id,)):
            row = db.cur_hris.fetchone() or {}
            try:
                subdomain_name = row.get('subdomain_name')
            except AttributeError:
                try:
                    subdomain_name = row[0]
                except Exception:
                    subdomain_name = None
        ads_list = []
        sql_ads = """
            SELECT ads_id, account_ads_id_1, fanpage, interest, country, daily_budget, status
            FROM data_media_fb_ads
            WHERE subdomain_id = %s
            ORDER BY ads_id DESC
        """
        if db.execute_query(sql_ads, (subdomain_id,)):
            ads_list = db.cur_hris.fetchall() or []
        accounts = []
        if db.execute_query("SELECT account_ads_id, account_name FROM master_account_ads ORDER BY account_name ASC"):
            accounts = db.cur_hris.fetchall() or []
        countries = []
        if db.execute_query("SELECT negara_kd, negara_nm, tier FROM master_negara ORDER BY negara_nm ASC"):
            countries = db.cur_hris.fetchall() or []
        context = {
            'user': admin,
            'active_portal_id': active_portal_id,
            'subdomain_id': subdomain_id,
            'subdomain_name': subdomain_name,
            'ads_list': ads_list,
            'accounts': accounts,
            'countries': countries,
        }
        return render(request, 'task/active/edit.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class ActiveAdsEditStatusView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        ads_id_raw = (request.POST.get('ads_id') or '').strip()
        status = (request.POST.get('status') or '').strip().lower()
        try:
            ads_id = int(ads_id_raw)
        except Exception:
            ads_id = None
        if not ads_id:
            return JsonResponse({'status': False, 'message': 'Ads ID wajib diisi.'}, status=400)
        if status not in ['on', 'off']:
            return JsonResponse({'status': False, 'message': 'Status tidak valid.'}, status=400)
        ok = db.execute_query(
            "UPDATE data_media_fb_ads SET status=%s, mdd=NOW() WHERE ads_id=%s",
            (status, ads_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui status.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class ActiveAdsEditDeleteView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        ads_id_raw = (request.POST.get('ads_id') or '').strip()
        try:
            ads_id = int(ads_id_raw)
        except Exception:
            ads_id = None
        if not ads_id:
            return JsonResponse({'status': False, 'message': 'Ads ID wajib diisi.'}, status=400)
        ok = db.execute_query("DELETE FROM data_media_fb_ads WHERE ads_id=%s", (ads_id,))
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal menghapus Ads.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class ActiveAdsEditCreateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        account_ads_id_1 = (request.POST.get('account_ads_id_1') or '').strip()
        fanpage = (request.POST.get('fanpage') or '').strip()
        interest = (request.POST.get('interest') or '').strip()
        country_vals = request.POST.getlist('country[]') or request.POST.getlist('country') or []
        daily_budget_raw = (request.POST.get('daily_budget') or '').strip()
        status = (request.POST.get('status') or '').strip().lower()
        mdb = str(admin.get('user_id', ''))[:36]
        mdb_name = admin.get('user_alias', '')
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        try:
            daily_budget = float(daily_budget_raw) if daily_budget_raw else None
        except Exception:
            daily_budget = None
        if not subdomain_id:
            return JsonResponse({'status': False, 'message': 'Subdomain wajib diisi.'}, status=400)
        if status not in ['on', 'off', '']:
            return JsonResponse({'status': False, 'message': 'Status tidak valid.'}, status=400)
        countries_text = None
        if country_vals:
            items = [str(v).strip() for v in country_vals if str(v).strip()]
            countries_text = ",".join(items) if items else None
        dup_name = None
        if fanpage:
            sql_dup = """
            SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name
            FROM data_media_fb_ads b
            INNER JOIN data_subdomain s ON s.subdomain_id = b.subdomain_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            WHERE b.fanpage = %s AND b.subdomain_id <> %s AND b.status = 'on'
            LIMIT 1
            """
            if db.execute_query(sql_dup, (fanpage, subdomain_id)):
                rr = db.cur_hris.fetchone() or {}
                try:
                    dup_name = rr.get('subdomain_name')
                except AttributeError:
                    try:
                        dup_name = rr[0]
                    except Exception:
                        dup_name = None
        if dup_name:
            return JsonResponse({'status': False, 'message': 'Fanpage telah digunakan untuk subdomain ' + str(dup_name or '')}, status=400)
        domain_id = None
        if db.execute_query("SELECT domain_id FROM data_subdomain WHERE subdomain_id=%s LIMIT 1", (subdomain_id,)):
            rr = db.cur_hris.fetchone() or {}
            try:
                domain_id = rr.get('domain_id')
            except AttributeError:
                try:
                    domain_id = rr[0]
                except Exception:
                    domain_id = None
        ok = db.execute_query(
            """
            INSERT INTO data_media_fb_ads
            (domain_id, subdomain_id, account_ads_id_1, fanpage, interest, country, daily_budget, status, mdb, mdb_name, mdd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (domain_id, subdomain_id, account_ads_id_1 or None, fanpage or None, interest or None, countries_text, daily_budget, status or None, mdb, mdb_name)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal menambah Ads.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class ActiveAdsEditUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        ads_id_raw = (request.POST.get('ads_id') or '').strip()
        account_ads_id_1 = (request.POST.get('account_ads_id_1') or '').strip()
        fanpage = (request.POST.get('fanpage') or '').strip()
        interest = (request.POST.get('interest') or '').strip()
        country_vals = request.POST.getlist('country[]') or request.POST.getlist('country') or []
        daily_budget_raw = (request.POST.get('daily_budget') or '').strip()
        try:
            ads_id = int(ads_id_raw)
        except Exception:
            ads_id = None
        try:
            daily_budget = float(daily_budget_raw) if daily_budget_raw else None
        except Exception:
            daily_budget = None
        if not ads_id:
            return JsonResponse({'status': False, 'message': 'Ads ID wajib diisi.'}, status=400)
        countries_text = None
        if country_vals:
            items = [str(v).strip() for v in country_vals if str(v).strip()]
            countries_text = ",".join(items) if items else None
        dup_name = None
        if fanpage:
            sql_dup = """
            SELECT CONCAT(s.subdomain, '.', d.domain) AS subdomain_name
            FROM data_media_fb_ads b
            INNER JOIN data_subdomain s ON s.subdomain_id = b.subdomain_id
            INNER JOIN data_domains d ON d.domain_id = s.domain_id
            WHERE b.fanpage = %s AND b.ads_id <> %s AND b.status = 'on'
            LIMIT 1
            """
            if db.execute_query(sql_dup, (fanpage, ads_id)):
                rr = db.cur_hris.fetchone() or {}
                try:
                    dup_name = rr.get('subdomain_name')
                except AttributeError:
                    try:
                        dup_name = rr[0]
                    except Exception:
                        dup_name = None
        if dup_name:
            return JsonResponse({'status': False, 'message': 'Fanpage telah digunakan untuk subdomain ' + str(dup_name or '')}, status=400)
        mdb = str(admin.get('user_id', ''))[:36]
        mdb_name = admin.get('user_alias', '')
        ok = db.execute_query(
            """
            UPDATE data_media_fb_ads
            SET account_ads_id_1=%s, fanpage=%s, interest=%s, country=%s, daily_budget=%s, mdb=%s, mdb_name=%s, mdd=NOW()
            WHERE ads_id=%s
            """,
            (account_ads_id_1 or None, fanpage or None, interest or None, countries_text, daily_budget, mdb, mdb_name, ads_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui Ads.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

@method_decorator(csrf_exempt, name='dispatch')
class ActiveProjectStatusUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        partner_id = (request.POST.get('partner_id') or '').strip()
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner ID wajib diisi.'}, status=400)
        ok = db.execute_query(
            "UPDATE data_media_partner SET status=%s, mdd=NOW() WHERE partner_id=%s",
            ('off', partner_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui status project.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})
@method_decorator(csrf_exempt, name='dispatch')
class NonactiveProjectStatusUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        partner_id = (request.POST.get('partner_id') or '').strip()
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner ID wajib diisi.'}, status=400)
        ok = db.execute_query(
            "UPDATE data_media_partner SET status=%s, mdd=NOW() WHERE partner_id=%s",
            ('completed', partner_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui status project.'}, status=500)
        db.commit()
        return JsonResponse({'status': True})

class TechnicalServerLookupView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        ip = (request.GET.get('ip') or request.POST.get('ip') or '').strip()
        partner_id = (request.GET.get('partner_id') or request.POST.get('partner_id') or '').strip()
        resp = {'status': True}
        srv = None
        if ip:
            sql_srv = """
                SELECT public_ipv4,
                       hostname,
                       label,
                       provider,
                       vcpu_count,
                       memory_gb,
                       ssh_user,
                       ssh_pass,
                       ssh_keys
                FROM data_servers
                WHERE public_ipv4 = %s
                ORDER BY COALESCE(mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            if db.execute_query(sql_srv, (ip,)):
                row = db.cur_hris.fetchone()
                srv = row or None
        resp['server'] = srv
        sd = None
        w = None
        subrows = []
        domains = []
        selected_domain_id = None
        if partner_id:
            q_domains = """
                SELECT d.domain_id, d.domain
                FROM data_media_partner_domain pd
                JOIN data_domains d ON d.domain_id = pd.domain_id
                WHERE pd.partner_id = %s
                ORDER BY d.domain ASC
            """
            if db.execute_query(q_domains, (partner_id,)):
                domains = db.cur_hris.fetchall() or []
            if not ip:
                sql_sds2 = """
                    SELECT s.subdomain, s.cloudflare, s.public_ipv4, s.website_id, s.domain_id,
                           w.website, w.website_user, w.website_pass
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    LEFT JOIN data_website w ON w.website_id = s.website_id
                    WHERE dmpd.partner_id = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 50
                """
                if db.execute_query(sql_sds2, (partner_id,)):
                    subrows = db.cur_hris.fetchall() or []
                    if subrows:
                        sd = subrows[0]
            if not ip:
                if sd and (sd.get('website_id') if hasattr(sd, 'get') else None):
                    sql_w = """
                        SELECT website, website_user, website_pass
                        FROM data_website
                        WHERE website_id = %s
                        LIMIT 1
                    """
                    wid = sd.get('website_id') if hasattr(sd, 'get') else None
                    if wid and db.execute_query(sql_w, (wid,)):
                        w = db.cur_hris.fetchone() or None
            try:
                selected_domain_id = sd.get('domain_id') if sd else None
            except AttributeError:
                selected_domain_id = None
        resp['subdomain'] = sd
        resp['website'] = w
        resp['subrows'] = (subrows if not ip else [])
        resp['domains'] = (domains if not ip else [])
        resp['selected_domain_id'] = selected_domain_id
        return JsonResponse(resp)

class TechnicalServerSaveView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        partner_id = request.POST.get('partner_id', '').strip()
        domain_id_raw = request.POST.get('domain_id', '').strip()
        try:
            selected_domain_id = int(domain_id_raw) if domain_id_raw else None
        except Exception:
            selected_domain_id = None
        srv_hostname = request.POST.get('srv_hostname', '').strip() or None
        srv_label = request.POST.get('srv_label', '').strip() or None
        srv_provider = request.POST.get('srv_provider', '').strip() or None
        srv_vcpu_count = request.POST.get('srv_vcpu_count', '').strip()
        srv_memory_gb = request.POST.get('srv_memory_gb', '').strip()
        srv_public_ipv4 = request.POST.get('srv_public_ipv4', '').strip() or None
        srv_ssh_user = request.POST.get('srv_ssh_user', '').strip() or None
        srv_ssh_pass = request.POST.get('srv_ssh_pass', '').strip() or None
        srv_ssh_keys = request.POST.get('srv_ssh_keys', '').strip() or None
        subdomains = [s.strip() for s in request.POST.getlist('sd_subdomain')]
        clouds = [s.strip() for s in request.POST.getlist('sd_cloudflare')]
        ips = [s.strip() for s in request.POST.getlist('sd_public_ipv4')]
        webs = [s.strip() for s in request.POST.getlist('ws_website')]
        wusers = [s.strip() for s in request.POST.getlist('ws_website_user')]
        wpasses = [s.strip() for s in request.POST.getlist('ws_website_pass')]
        try:
            vcpu = int(srv_vcpu_count) if srv_vcpu_count else 0
        except Exception:
            vcpu = None
        try:
            mem = int(srv_memory_gb) if srv_memory_gb else 0
        except Exception:
            mem = None
        if not partner_id:
            return JsonResponse({'status': False, 'message': 'Partner wajib diisi.'}, status=400)
        if not (subdomains or clouds or ips or webs or wusers or wpasses):
            return JsonResponse({'status': False, 'message': 'Data subdomain/website belum diisi.'}, status=400)
        def resolve_domain_id(ipv, sub):
            did = None
            if ipv:
                sql = """
                    SELECT s.domain_id
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    WHERE dmpd.partner_id = %s AND s.public_ipv4 = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql, (partner_id, ipv)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            if not did and sub:
                sql2 = """
                    SELECT s.domain_id
                    FROM data_media_partner_domain dmpd
                    JOIN data_subdomain s ON s.domain_id = dmpd.domain_id
                    WHERE dmpd.partner_id = %s AND s.subdomain = %s
                    ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql2, (partner_id, sub)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            if not did:
                sql3 = """
                    SELECT pd.domain_id
                    FROM data_media_partner_domain pd
                    WHERE pd.partner_id = %s
                    ORDER BY COALESCE(pd.mdd, '0000-00-00 00:00:00') DESC
                    LIMIT 1
                """
                if db.execute_query(sql3, (partner_id,)):
                    rr = db.cur_hris.fetchone() or {}
                    try:
                        did = rr.get('domain_id')
                    except AttributeError:
                        try:
                            did = rr[0]
                        except Exception:
                            did = None
            return did
        server_id = None
        if srv_public_ipv4:
            if db.execute_query("SELECT server_id FROM data_servers WHERE public_ipv4 = %s LIMIT 1", (srv_public_ipv4,)):
                row = db.cur_hris.fetchone() or {}
                try:
                    server_id = row.get('server_id')
                except AttributeError:
                    try:
                        server_id = row[0]
                    except Exception:
                        server_id = None
            if not server_id:
                sql_i = """
                    INSERT INTO data_servers
                    (hostname, label, provider, vcpu_count, memory_gb, public_ipv4, ssh_user, ssh_pass, ssh_keys, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """
                if not db.execute_query(sql_i, (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_public_ipv4, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''))):
                    return JsonResponse({'status': False, 'message': 'Gagal menambahkan server.'}, status=500)
                else:
                    db.commit()
                    try:
                        server_id = db.cur_hris.lastrowid
                    except Exception:
                        server_id = None
        else:
            # without server IPv4, we still allow subdomain/website insert, but notify client
            pass
        website_ids = []
        inserted_subdomains = 0
        row_errors = []
        for i in range(max(len(subdomains), len(clouds), len(ips), len(webs), len(wusers), len(wpasses))):
            sd_sub = (subdomains[i] if i < len(subdomains) else '').strip()
            sd_cf = (clouds[i] if i < len(clouds) else '').strip()
            sd_ip = (ips[i] if i < len(ips) else '').strip() or (srv_public_ipv4 or '')
            w_site = (webs[i] if i < len(webs) else '').strip()
            w_user = (wusers[i] if i < len(wusers) else '').strip()
            w_pass = (wpasses[i] if i < len(wpasses) else '').strip()
            if not (sd_sub or sd_cf or sd_ip or w_site or w_user or w_pass):
                continue
            did = resolve_domain_id(sd_ip, sd_sub)
            if not did and selected_domain_id:
                did = selected_domain_id
            if not did:
                row_errors.append({'row': i+1, 'message': 'Domain untuk baris ini tidak dapat ditentukan.'})
                continue
            # domain_id provided via POST or resolved; no need to insert partner-domain mapping here
            wid = None
            if w_site or w_user or w_pass:
                if not db.execute_query("INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, %s, NOW())", (
                    w_site, w_user, w_pass, admin.get('user_id',''), admin.get('user_alias','')
                )):
                    row_errors.append({'row': i+1, 'message': 'Gagal menambahkan data website.'})
                else:
                    db.commit()
                    try:
                        wid = db.cur_hris.lastrowid
                    except Exception:
                        wid = None
            sd_id = None
            sql_sd_exist = """
                SELECT s.subdomain_id
                FROM data_subdomain s
                WHERE s.domain_id = %s AND (s.subdomain = %s OR s.public_ipv4 = %s)
                ORDER BY COALESCE(s.mdd, '0000-00-00 00:00:00') DESC
                LIMIT 1
            """
            sql_sd_i = """
                INSERT INTO data_subdomain (subdomain, domain_id, website_id, cloudflare, public_ipv4, mdb, mdb_name, mdd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """
            if not db.execute_query(sql_sd_i, (sd_sub or None, did, wid, sd_cf or None, sd_ip or None, admin.get('user_id',''), admin.get('user_alias',''))):
                row_errors.append({'row': i+1, 'message': 'Gagal menambahkan subdomain.'})
            else:
                db.commit()
                inserted_subdomains += 1
            website_ids.append(wid)
        if inserted_subdomains == 0:
            msg = 'Tidak ada subdomain yang berhasil ditambahkan.'
            if row_errors:
                try:
                    msg = row_errors[0].get('message') or msg
                except Exception:
                    msg = msg
            return JsonResponse({'status': False, 'message': msg, 'errors': row_errors}, status=400)
        return JsonResponse({'status': True, 'server_id': server_id, 'website_ids': website_ids})
class TechnicalSubrowLookupView(View):
    def get(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        partner_id = (request.GET.get('partner_id') or '').strip()
        subdomain_id_raw = (request.GET.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            subdomain_id = None
        db = data_mysql()
        resp = {'status': True}
        subrow = None
        domain = None
        server = None
        website = None
        if subdomain_id:
            sql = """
                SELECT s.subdomain_id, s.subdomain, s.cloudflare, s.public_ipv4, s.domain_id, s.website_id,
                       d.domain
                FROM data_subdomain s
                JOIN data_domains d ON d.domain_id = s.domain_id
                WHERE s.subdomain_id = %s
                LIMIT 1
            """
            if db.execute_query(sql, (subdomain_id,)):
                subrow = db.cur_hris.fetchone() or None
            if subrow:
                if subrow.get('website_id'):
                    if db.execute_query("SELECT website, website_user, website_pass, article_deadline FROM data_website WHERE website_id = %s LIMIT 1", (subrow.get('website_id'),)):
                        website = db.cur_hris.fetchone() or None
                if subrow.get('public_ipv4'):
                    q = """
                        SELECT public_ipv4, hostname, label, provider, vcpu_count, memory_gb, ssh_user, ssh_pass, ssh_keys
                        FROM data_servers
                        WHERE public_ipv4 = %s
                        LIMIT 1
                    """
                    if db.execute_query(q, (subrow.get('public_ipv4'),)):
                        server = db.cur_hris.fetchone() or None
                domain = {'domain_id': subrow.get('domain_id'), 'domain': subrow.get('domain')}
        resp['subrow'] = subrow
        resp['domain'] = domain
        resp['server'] = server
        resp['website'] = website
        return JsonResponse(resp)

class TechnicalSubrowUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        admin = request.session.get('hris_admin', {})
        subdomain_id_raw = request.POST.get('subdomain_id', '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            return JsonResponse({'status': False, 'message': 'Subdomain tidak valid'}, status=400)
        srv_hostname = request.POST.get('srv_hostname', '').strip() or None
        srv_label = request.POST.get('srv_label', '').strip() or None
        srv_provider = request.POST.get('srv_provider', '').strip() or None
        srv_vcpu_count = request.POST.get('srv_vcpu_count', '').strip()
        srv_memory_gb = request.POST.get('srv_memory_gb', '').strip()
        srv_public_ipv4 = request.POST.get('srv_public_ipv4', '').strip() or None
        srv_ssh_user = request.POST.get('srv_ssh_user', '').strip() or None
        srv_ssh_pass = request.POST.get('srv_ssh_pass', '').strip() or None
        srv_ssh_keys = request.POST.get('srv_ssh_keys', '').strip() or None
        sd_subdomain = request.POST.get('sd_subdomain', '').strip() or None
        sd_cloudflare = request.POST.get('sd_cloudflare', '').strip() or None
        sd_public_ipv4 = request.POST.get('sd_public_ipv4', '').strip() or srv_public_ipv4
        ws_website = request.POST.get('ws_website', '').strip() or None
        ws_website_user = request.POST.get('ws_website_user', '').strip() or None
        ws_website_pass = request.POST.get('ws_website_pass', '').strip() or None
        try:
            vcpu = int(srv_vcpu_count) if srv_vcpu_count else None
        except Exception:
            vcpu = None
        try:
            mem = int(srv_memory_gb) if srv_memory_gb else None
        except Exception:
            mem = None
        # get current subdomain
        sql_sd = """
            SELECT domain_id, website_id
            FROM data_subdomain
            WHERE subdomain_id = %s
            LIMIT 1
        """
        if not db.execute_query(sql_sd, (subdomain_id,)):
            return JsonResponse({'status': False, 'message': 'Subdomain tidak ditemukan'}, status=404)
        curr = db.cur_hris.fetchone() or {}
        domain_id = curr.get('domain_id')
        curr_wid = curr.get('website_id')
        errors = []
        # upsert server by public ipv4
        server_id = None
        if srv_public_ipv4:
            if db.execute_query("SELECT server_id FROM data_servers WHERE public_ipv4 = %s LIMIT 1", (srv_public_ipv4,)):
                r = db.cur_hris.fetchone() or {}
                try:
                    server_id = r.get('server_id')
                except AttributeError:
                    try:
                        server_id = r[0]
                    except Exception:
                        server_id = None
            if server_id:
                ok = db.execute_query(
                    """
                    UPDATE data_servers SET hostname=%s, label=%s, provider=%s, vcpu_count=%s, memory_gb=%s,
                        ssh_user=%s, ssh_pass=%s, ssh_keys=%s, mdb=%s, mdb_name=%s, mdd=NOW()
                    WHERE server_id=%s
                    """,
                    (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''), server_id)
                )
                if not ok:
                    errors.append({'section': 'server', 'message': 'Gagal memperbarui server.'})
                else:
                    db.commit()
            else:
                if not db.execute_query(
                    """
                    INSERT INTO data_servers (hostname, label, provider, vcpu_count, memory_gb, public_ipv4, ssh_user, ssh_pass, ssh_keys, mdb, mdb_name, mdd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (srv_hostname, srv_label, srv_provider, vcpu, mem, srv_public_ipv4, srv_ssh_user, srv_ssh_pass, srv_ssh_keys, admin.get('user_id',''), admin.get('user_alias',''))
                ):
                    errors.append({'section': 'server', 'message': 'Gagal menambahkan server.'})
                else:
                    db.commit()
        # website update or insert
        wid = curr_wid
        if ws_website or ws_website_user or ws_website_pass:
            if wid:
                ok = db.execute_query("UPDATE data_website SET website=%s, website_user=%s, website_pass=%s, mdb=%s, mdb_name=%s, mdd=NOW() WHERE website_id=%s", (
                    ws_website, ws_website_user, ws_website_pass, admin.get('user_id',''), admin.get('user_alias',''), wid
                ))
                if not ok:
                    errors.append({'section': 'website', 'message': 'Gagal memperbarui website.'})
                else:
                    db.commit()
            else:
                if not db.execute_query("INSERT INTO data_website (website, website_user, website_pass, mdb, mdb_name, mdd) VALUES (%s, %s, %s, %s, %s, NOW())", (
                    ws_website, ws_website_user, ws_website_pass, admin.get('user_id',''), admin.get('user_alias','')
                )):
                    errors.append({'section': 'website', 'message': 'Gagal menambahkan website.'})
                else:
                    db.commit()
                    try:
                        wid = db.cur_hris.lastrowid
                    except Exception:
                        wid = None
        # update subdomain
        ok = db.execute_query(
            """
            UPDATE data_subdomain SET subdomain=%s, cloudflare=%s, public_ipv4=%s, website_id=%s, mdb=%s, mdb_name=%s, mdd=NOW()
            WHERE subdomain_id=%s
            """,
            (sd_subdomain, sd_cloudflare, sd_public_ipv4, wid, admin.get('user_id',''), admin.get('user_alias',''), subdomain_id)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui subdomain.'}, status=500)
        db.commit()
        if errors:
            return JsonResponse({'status': False, 'message': 'Sebagian data gagal diperbarui.', 'errors': errors}, status=400)
        return JsonResponse({'status': True, 'subdomain_id': subdomain_id})

class TechnicalSubrowDeleteView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            return JsonResponse({'status': False, 'message': 'Subdomain tidak valid'}, status=400)
        wid = None
        did = None
        sql = """
            SELECT website_id, domain_id
            FROM data_subdomain
            WHERE subdomain_id = %s
            LIMIT 1
        """
        if db.execute_query(sql, (subdomain_id,)):
            r = db.cur_hris.fetchone() or {}
            try:
                wid = r.get('website_id')
                did = r.get('domain_id')
            except AttributeError:
                try:
                    wid = r[0]
                    did = r[1] if len(r) > 1 else None
                except Exception:
                    wid = None
                    did = None
        db.execute_query("DELETE FROM data_website_niche WHERE subdomain_id = %s", (subdomain_id,))
        db.commit()
        if wid:
            if did:
                try:
                    if db.execute_query("SELECT website_id FROM data_domains WHERE domain_id = %s LIMIT 1", (did,)):
                        dr = db.cur_hris.fetchone() or {}
                        curr_wid = None
                        try:
                            curr_wid = dr.get('website_id')
                        except AttributeError:
                            try:
                                curr_wid = dr[0]
                            except Exception:
                                curr_wid = None
                        if curr_wid and str(curr_wid) == str(wid):
                            db.execute_query("UPDATE data_domains SET website_id = NULL WHERE domain_id = %s", (did,))
                            db.commit()
                except Exception:
                    pass
            db.execute_query("UPDATE data_subdomain SET website_id = NULL WHERE subdomain_id = %s", (subdomain_id,))
            db.commit()
            db.execute_query("DELETE FROM data_website WHERE website_id = %s", (wid,))
            db.commit()
        ok = db.execute_query("DELETE FROM data_subdomain WHERE subdomain_id = %s", (subdomain_id,))
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal menghapus subdomain'}, status=500)
        db.commit()
        return JsonResponse({'status': True})
class TechnicalWebsiteDeadlineUpdateView(View):
    def post(self, request):
        if 'hris_admin' not in request.session:
            return JsonResponse({'status': False, 'message': 'Unauthorized'}, status=401)
        admin = request.session.get('hris_admin', {})
        db = data_mysql()
        subdomain_id_raw = (request.POST.get('subdomain_id') or '').strip()
        deadline = (request.POST.get('article_deadline') or '').strip()
        try:
            subdomain_id = int(subdomain_id_raw)
        except Exception:
            return JsonResponse({'status': False, 'message': 'Subdomain tidak valid'}, status=400)
        wid = None
        if db.execute_query("SELECT website_id FROM data_subdomain WHERE subdomain_id = %s LIMIT 1", (subdomain_id,)):
            r = db.cur_hris.fetchone() or {}
            try:
                wid = r.get('website_id')
            except AttributeError:
                try:
                    wid = r[0]
                except Exception:
                    wid = None
        if not wid:
            return JsonResponse({'status': False, 'message': 'Website tidak tersedia untuk subdomain ini'}, status=400)
        ok = db.execute_query(
            "UPDATE data_website SET article_deadline=%s, mdb=%s, mdb_name=%s, mdd=NOW() WHERE website_id=%s",
            (deadline if deadline else None, admin.get('user_id',''), admin.get('user_alias',''), wid)
        )
        if not ok:
            return JsonResponse({'status': False, 'message': 'Gagal memperbarui deadline'}, status=500)
        db.commit()
        return JsonResponse({'status': True, 'website_id': wid})
