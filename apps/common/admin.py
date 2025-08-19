import csv
import os
from collections.abc import Iterable

from django import forms
from django.contrib import admin
from django.db import transaction
from django.http import HttpResponseRedirect, HttpResponse
from django.urls import reverse, path
from django.utils import dateformat
from django.utils.html import format_html

from apps.common import ReportTypes, UpdatingTypes
from apps.common.models import UpdateHistory, UpdateHistoryReport
from apps.common.tasks import (
    update_esep_db_task,
    update_kok_db_task,
    run_rollback_task,
)

admin.site.site_header = "Updater Ninja Project"
admin.site.site_title = "Updater Ninja Project"
admin.site.index_title = ""


class UpdateHistoryReportInline(admin.TabularInline):
    model = UpdateHistoryReport
    extra = 0
    fields = (
        'report_type',
        'get_data_json',
        'get_csv_report',
    )
    readonly_fields = ('get_csv_report', 'get_data_json')

    def get_csv_report(self, obj):
        if (
            obj is None
            or not obj.data_json
            or (isinstance(obj.data_json, dict) and obj.data_json.get('amount_total') == 0)
            or (isinstance(obj.data_json, list) and len(obj.data_json) == 0)
        ):
            return '-'

        return format_html(
            '<a href="{}">Скачать csv отчет</a>',
            reverse('admin:common_updatehistory_download_report_csv', args=[obj.id])
        )

    def get_data_json(self, obj):
        if isinstance(obj.data_json, list) and len(obj.data_json) > 100:
            return obj.data_json[:10] + ['...']
        elif isinstance(obj.data_json, dict) and obj.data_json.get('amount_total') and obj.data_json.get('amount_total') > 10:
            return obj.data_json.get("items", [])[:10] + ['...']
        return obj.data_json


class UpdateHistoryForm(forms.ModelForm):
    class Meta:
        model = UpdateHistory
        fields = ['company_id', 'updating_type', 'data_file', 'is_automatic', 'status', 'status_reason']

    def clean_data_file(self):
        file = self.cleaned_data.get('data_file')
        if file:
            ext = os.path.splitext(file.name)[1]
            if ext.lower() not in ['.csv']:
                raise forms.ValidationError('Поддерживается только .csv формат')
        return file


@admin.register(UpdateHistory)
class UpdateHistoryAdmin(admin.ModelAdmin):
    fields = (
        'created_at',
        'completed_at',
        'rolled_back_at',
        'company_id',
        'updating_type',
        'is_automatic',
        'data_file',
        'status',
        'status_reason',
    )
    list_display = ('id', 'created_at', 'company_id', 'status', 'updating_type', 'is_automatic', 'data_file')
    list_filter = (
        'status',
        'updating_type',
        'is_automatic',
        'company_id',
    )
    search_fields = ('id',)
    readonly_fields = ('is_automatic',)
    date_hierarchy = "created_at"

    inlines = [UpdateHistoryReportInline]
    form = UpdateHistoryForm

    # ---------- helpers ----------
    def _write_csv_bom(self, response: HttpResponse) -> None:
        """Excel ожидает UTF-8 с BOM, иначе кириллица ломается."""
        response.write("\ufeff")

    def _dict_changes_rows(self, item: dict, kind: str):
        """
        Превращает детальные элементы (dict) в плоские строки для CSV.
        kind ∈ {"device_updates","device_deletions","device_added","bankbook_updates"}
        """
        rows = []
        changes = item.get("changes", []) or []

        if kind == "device_updates":
            # header: account_number;device_id;device_number;field;old;new
            base = [
                item.get("account_number", ""),
                item.get("device_id", ""),
                item.get("device_number", ""),
            ]
            for ch in changes:
                rows.append(base + [ch.get("field", ""), ch.get("old", ""), ch.get("new", "")])

        elif kind in ("device_deletions", "device_added"):
            # header: account_number;device_id;device_number;field;old;new;action
            base = [
                item.get("account_number", ""),
                item.get("device_id", ""),
                item.get("device_number", ""),
            ]
            action = item.get("action", "deleted" if kind == "device_deletions" else "created")
            if changes:
                for ch in changes:
                    rows.append(base + [ch.get("field", ""), ch.get("old", ""), ch.get("new", ""), action])
            else:
                rows.append(base + ["", "", "", action])

        elif kind == "bankbook_updates":
            # header: account_number;bankbook_id;field;old;new
            base = [
                item.get("account_number", ""),
                item.get("bankbook_id", ""),
            ]
            for ch in changes:
                rows.append(base + [ch.get("field", ""), ch.get("old", ""), ch.get("new", "")])

        return rows
    # -----------------------------

    def change_view(self, request, object_id, form_url="", extra_context=None):
        obj = self.get_object(request, object_id)
        extra_context = extra_context or {}
        if obj:
            download_total_report_url = reverse('admin:common_updatehistory_download_total_report_csv', args=[obj.id])
            extra_context['download_total_report_url'] = download_total_report_url
        return super().change_view(request, object_id, form_url, extra_context=extra_context)

    def response_add(self, request, obj, post_url_continue=None):
        self.message_user(request, "Synchronizing data...")
        def enqueue():
            # 7 -> ACA, 10 -> Кокшетау
            if getattr(obj, "company_id", 7) == 10:
                update_kok_db_task.delay(update_obj_pk=obj.pk)
            else:
                update_esep_db_task.delay(update_obj_pk=obj.pk)
        transaction.on_commit(enqueue)
        return super().response_add(request, obj, post_url_continue=post_url_continue)

    def response_change(self, request, obj):
        if "run_rollback_changes" in request.POST:
            self.message_user(request, "Rolling back last changes")
            run_rollback_task.delay(obj.pk)
            return HttpResponseRedirect(".")
        return super().response_change(request, obj)

    def get_urls(self):
        url_name = '%s_%s_download_report_csv' % (self.model._meta.app_label, self.model._meta.model_name)
        url_name2 = '%s_%s_download_total_report_csv' % (self.model._meta.app_label, self.model._meta.model_name)
        urls = [
            path('report_csv/<int:pk>/download', self.download_report_csv, name=url_name),
            path('report_csv/<int:pk>/download_total', self.download_total_report_csv, name=url_name2),
        ]
        return super().get_urls() + urls

    # ----------- CSV: один отчёт -----------
    def download_report_csv(self, request, pk: int):
        report = UpdateHistoryReport.objects.get(pk=pk)
        response = HttpResponse(content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = f'attachment; filename="update_history_report-{report.report_type}.csv"'
        self._write_csv_bom(response)
        csv_writer = csv.writer(response, delimiter=';')

        data_json = report.data_json or {}
        items = data_json.get('items', []) if isinstance(data_json, dict) else data_json
        if not items:
            return response

        # Особый режим (оставлено как было): для BY_FILE тянем строки из исходного CSV
        if (
            report.report_type == ReportTypes.IN_VODOKANAL_NOT_IN_ESEP
            and report.update_history.updating_type == UpdatingTypes.BY_FILE
            and report.update_history.data_file
        ):
            with open(report.update_history.data_file.path, 'r', encoding='windows-1251') as file:
                reader = csv.reader(file, delimiter=';')
                for i, row in enumerate(reader):
                    if i == 0 or (row[10] or '').strip() in items:
                        csv_writer.writerow(row)
            return response

        first = items[0]

        # ===== ДЕТАЛЬНЫЕ ОТЧЁТЫ (элементы-словари) =====
        if isinstance(first, dict):
            if report.report_type == ReportTypes.DETAILED_DEVICE_UPDATES:
                csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new"))
                for it in items:
                    for row in self._dict_changes_rows(it, "device_updates"):
                        csv_writer.writerow(row)

            elif report.report_type == ReportTypes.DETAILED_DEVICE_DELETIONS:
                csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new", "action"))
                for it in items:
                    for row in self._dict_changes_rows(it, "device_deletions"):
                        csv_writer.writerow(row)

            elif report.report_type == ReportTypes.DETAILED_DEVICE_ADDED:
                csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new", "action"))
                for it in items:
                    for row in self._dict_changes_rows(it, "device_added"):
                        csv_writer.writerow(row)

            elif report.report_type == ReportTypes.DETAILED_BANKBOOK_UPDATES:
                csv_writer.writerow(("account_number", "bankbook_id", "field", "old", "new"))
                for it in items:
                    for row in self._dict_changes_rows(it, "bankbook_updates"):
                        csv_writer.writerow(row)

            else:
                # safety-net на случай новых типов
                csv_writer.writerow(("raw_json",))
                for it in items:
                    csv_writer.writerow((str(it),))
            return response

        # ===== НЕ-детальные отчёты (как раньше, но без ловушки Iterable для dict) =====
        if isinstance(first, str):
            csv_writer.writerow(("номер ЛС",))
            for item in items:
                csv_writer.writerow((item,))
        elif isinstance(first, int):
            csv_writer.writerow(("ID ПУ",))
            for item in items:
                csv_writer.writerow((item,))
        elif isinstance(first, (list, tuple, set)):
            if report.report_type == ReportTypes.REMOVED_REDUNDANT_DEVICES:
                csv_writer.writerow(("номер ЛС", "номер ПУ", "ID ПУ"))
            else:
                csv_writer.writerow(("номер ЛС", "номер ПУ"))
            for row in items:
                csv_writer.writerow(list(row))
        else:
            # неизвестный формат — просто дамп строкой
            csv_writer.writerow(("value",))
            for item in items:
                csv_writer.writerow((str(item),))

        return response

    # ----------- CSV: общий отчёт -----------
    def download_total_report_csv(self, request, pk: int):
        """
        Собирает все отчёты в один CSV.
        Для каждого типа пишет заголовок секции, затем собственный header и данные.
        """
        update_instance = UpdateHistory.objects.get(pk=pk)
        timestamp = dateformat.format(update_instance.created_at, 'Y-m-d_H:i')
        response = HttpResponse(content_type='text/csv; charset=utf-8')
        response['Content-Disposition'] = f'attachment; filename="update_history_{timestamp}_total_report.csv"'
        self._write_csv_bom(response)
        csv_writer = csv.writer(response, delimiter=';')

        # helper для секции
        def write_section_title(title: str):
            csv_writer.writerow(())
            csv_writer.writerow((title,))
            csv_writer.writerow(())

        # порядок можно задать явно, чтобы было привычно
        ordered_reports = list(update_instance.reports.all().order_by('report_type'))

        for report in ordered_reports:
            label = getattr(ReportTypes, report.report_type).label if hasattr(ReportTypes, report.report_type) else report.report_type
            data_json = report.data_json or {}
            items = data_json.get('items', []) if isinstance(data_json, dict) else data_json
            if not items:
                continue

            write_section_title(label)

            first = items[0]

            # Детальные форматы
            if isinstance(first, dict):
                if report.report_type == ReportTypes.DETAILED_DEVICE_UPDATES:
                    csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new"))
                    for it in items:
                        for row in self._dict_changes_rows(it, "device_updates"):
                            csv_writer.writerow(row)

                elif report.report_type == ReportTypes.DETAILED_DEVICE_DELETIONS:
                    csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new", "action"))
                    for it in items:
                        for row in self._dict_changes_rows(it, "device_deletions"):
                            csv_writer.writerow(row)

                elif report.report_type == ReportTypes.DETAILED_DEVICE_ADDED:
                    csv_writer.writerow(("account_number", "device_id", "device_number", "field", "old", "new", "action"))
                    for it in items:
                        for row in self._dict_changes_rows(it, "device_added"):
                            csv_writer.writerow(row)

                elif report.report_type == ReportTypes.DETAILED_BANKBOOK_UPDATES:
                    csv_writer.writerow(("account_number", "bankbook_id", "field", "old", "new"))
                    for it in items:
                        for row in self._dict_changes_rows(it, "bankbook_updates"):
                            csv_writer.writerow(row)

                else:
                    csv_writer.writerow(("raw_json",))
                    for it in items:
                        csv_writer.writerow((str(it),))
                continue

            # Неструктурные (старые) форматы
            if isinstance(first, str):
                csv_writer.writerow(("номер ЛС",))
                for item in items:
                    csv_writer.writerow((item,))
            elif isinstance(first, int):
                csv_writer.writerow(("ID ПУ",))
                for item in items:
                    csv_writer.writerow((item,))
            elif isinstance(first, (list, tuple, set)):
                if report.report_type == ReportTypes.REMOVED_REDUNDANT_DEVICES:
                    csv_writer.writerow(("номер ЛС", "номер ПУ", "ID ПУ"))
                else:
                    csv_writer.writerow(("номер ЛС", "номер ПУ"))
                for row in items:
                    csv_writer.writerow(list(row))
            else:
                csv_writer.writerow(("value",))
                for item in items:
                    csv_writer.writerow((str(item),))

        # Блок исходного CSV для «нет ЛС в Есеп» при BY_FILE (как было)
        no_accounts_report = update_instance.reports.filter(report_type=ReportTypes.IN_VODOKANAL_NO_ACCOUNT_IN_ESEP).first()
        if (
            no_accounts_report
            and isinstance(no_accounts_report.data_json, dict)
            and no_accounts_report.data_json.get('amount_total') > 0
            and update_instance.updating_type == UpdatingTypes.BY_FILE
            and update_instance.data_file
        ):
            csv_writer.writerow(())
            csv_writer.writerow((getattr(ReportTypes, no_accounts_report.report_type).label,))
            with open(update_instance.data_file.path, 'r', encoding='windows-1251') as file:
                reader = csv.reader(file, delimiter=';')
                target = set(no_accounts_report.data_json.get('items', []))
                for i, row in enumerate(reader):
                    if i == 0 or (row[10] or '').strip() in target:
                        csv_writer.writerow(row)

        return response
