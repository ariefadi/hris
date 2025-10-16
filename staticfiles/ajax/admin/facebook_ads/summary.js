/**
 * Reference Ajax Summary Facebook Ads
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
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#tanggal_dari').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true
    });
    $('#tanggal_sampai').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true
    });
    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected = $("#select_account").val() || '%';
        var data_account = selected ? selected : '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_account!="")
        {
            destroy_chart_summary_facebook()
            table_chart_summary_facebook(tanggal_dari, tanggal_sampai, data_account)
        }    
    });
});

function table_chart_summary_facebook(tanggal_dari, tanggal_sampai, data_account) {
    $.ajax({
        url: '/management/admin/page_summary_facebook?tanggal_dari='+tanggal_dari+'&tanggal_sampai='+tanggal_sampai+'&data_account='+data_account,
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_summary) {
            $('#overlay').fadeOut(500);
            const data = data_summary.data_summary;
            const data_total = data_summary.data_total;
            const data_jumlah = data_summary.data_jumlah;
            data_jumlah.forEach(function(item, index) {
                item.date = item.date.split('-').reverse().join('-');
                item.impressions = Number(item.impressions).toLocaleString('id-ID');
                item.reach = Number(item.reach).toLocaleString('id-ID');
                item.clicks = Number(item.clicks).toLocaleString('id-ID');
            });
            if (data_total.length > 0 && data_total[0]) {
                let spend = 'Rp ' + Number(data_total[0]['spend']).toLocaleString('id-ID');
                let data_cpr = data_total[0]['cpr'];
                let cpr_number = parseFloat(data_cpr);
                let cpr = "Rp. "+ cpr_number.toFixed(0).replace(',', '.');
                let data_cpc = data_total[0]['cpc'];
                let cpc_number = parseFloat(data_cpc);
                let cpc = "Rp. "+ cpc_number.toFixed(0).replace(',', '.');
                $('#spend').text(spend);
                $('#clicks').text(Number(data_total[0]['clicks']).toLocaleString('id-ID'));
                $('#cpr').text(cpr);
                $('#cpc').text(cpc);
            } else {
                $('#spend').text('Rp. 0');
                $('#clicks').text('0');
                $('#cpr').text('Rp. 0');
                $('#cpc').text('Rp. 0');
                console.warn("Data total kosong, tidak ada data untuk ditampilkan.");
            }

            Highcharts.chart('container', {
                title: {
                    text: 'Data Detail Account Facebook Ads'
                },
                series: [
                    {
                        type: 'treegraph',
                        data,
                        tooltip: {
                            pointFormatter: function () {
                                const name = this.name || '-';
                                const budget = this.budget ? 'Rp ' + Number(this.budget).toLocaleString('id-ID') : '-';
                                const spend = this.custom?.spend ? 'Rp ' + Number(this.custom.spend).toLocaleString('id-ID') : '-';
                                return `
                                    <b>${name}</b><br/>
                                    Spend: ${spend}<br/>
                                    Budget: ${budget}
                                `;
                            }
                        },
                        marker: {
                            symbol: 'rect',
                            width: 225,
                            height: 25
                        },
                        borderRadius: 2,
                        dataLabels: {
                            pointFormat: '{point.name}',
                            style: {
                                fontSize: '8px',
                                fontWeight: 'bold',
                                color: '#FFFFFF'
                            }
                        },
                        levels: [
                            {
                                level: 1,
                                levelIsConstant: false
                            },
                            {
                                level: 2,
                                colorByPoint: true
                            },
                            {
                                level: 3,
                                colorVariation: {
                                    key: 'brightness',
                                    to: -0.5
                                }
                            },
                            {
                                level: 4,
                                colorVariation: {
                                    key: 'brightness',
                                    to: 1
                                }
                            }
                        ]
                    }
                ]
            });
            Highcharts.chart('container-activity', {
                chart: {
                    type: 'spline'
                },
                title: {
                    text: 'Summary Perbandingan Impression, Reach, dan Clicks'
                },
                xAxis: {
                    categories: data_jumlah.map(item => item.date),
                    title: {
                        text: 'Data Range Tanggal'
                    },
                    labels: {
                        rotation: -15
                    }
                },
                yAxis: {
                    title: {
                        text: 'Jumlah Data'
                    },
                    labels: {
                        formatter: function () {
                            return this.value.toLocaleString('id-ID');
                        }
                    }
                },
                tooltip: {
                    shared: true,
                    crosshairs: true,
                    formatter: function () {
                        const idx = this.points[0].point.index;
                        const tanggal = data_jumlah[idx].date;
                        let tooltip = `<b>Tanggal: ${tanggal}</b><br/>`;
                        this.points.forEach(point => {
                            tooltip += `<span style="color:${point.color}">\u25CF</span> ${point.series.name}: <b>${point.y.toLocaleString('id-ID')}</b><br/>`;
                        });
                        return tooltip;
                    }
                },
                plotOptions: {
                    spline: {
                        dataLabels: {
                            enabled: true,
                            formatter: function () {
                                return this.y.toLocaleString('id-ID');
                            }
                        },
                        enableMouseTracking: true
                    }
                },
                series: [{
                    name: 'Impression',
                    data: data_jumlah.map(item => parseInt(item.impressions.replace(/\./g, '')))
                }, {
                    name: 'Reach',
                    data: data_jumlah.map(item => parseInt(item.reach.replace(/\./g, '')))
                }, {
                    name: 'Clicks',
                    data: data_jumlah.map(item => parseInt(item.clicks.replace(/\./g, '')))
                }]
            });
        }
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');

function destroy_chart_summary_facebook() {
    if ($('#select_campaign').hasClass("select2-hidden-accessible")) {
        $('#select_campaign').select2('destroy');
    }
}