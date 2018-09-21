from django.conf import settings
from django.contrib import messages
from django.http.response import HttpResponseRedirect
from django.urls.base import reverse
from django.utils.safestring import mark_safe
from django.views.generic.base import TemplateView
from edc_base.view_mixins import EdcBaseViewMixin
from edc_pdutils.exportables import Exportables

from ..archive_exporter import ArchiveExporter
from ..model_options import ModelOptions


class NothingToExport(Exception):
    pass


class ExportModelsViewError(Exception):
    pass


class ExportModelsView(EdcBaseViewMixin, TemplateView):

    post_action_url = 'edc_pdutils:home_url'
    template_name = f'edc_pdutils/bootstrap{settings.EDC_BOOTSTRAP}/home.html'

    def __init__(self, *args, **kwargs):
        self._selected_models_from_post = None
        self._selected_models_from_session = None
        super().__init__(*args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        if self.request.session.get('selected_models'):
            context.update(
                selected_models=[
                    ModelOptions(**dct) for dct in self.request.session[
                        'selected_models']])
        return context

    def post(self, request, *args, **kwargs):
        if not request.user.email:
            user_url = reverse('admin:auth_user_change',
                               args=(request.user.id, ))
            messages.error(
                request,
                mark_safe(
                    f'Your account does not include an email address. '
                    f'Please update your <a href="{user_url}">user account</a> '
                    f'and try again.'))
        else:
            try:
                self.export_models(request=request, email_to_user=True)
            except NothingToExport:
                self.request.session['selected_models'] = (
                    self.get_selected_models_from_post())
        url = reverse(self.post_action_url, kwargs=self.kwargs)
        return HttpResponseRedirect(url)

    def check_export_permissions(self, selected_models):
        return selected_models

    def export_models(self, request=None, email_to_user=None):
        exporter = ArchiveExporter(email_to_user=email_to_user)
        selected_models = self.get_selected_models_from_session()
        selected_models = self.check_export_permissions(selected_models)
        exporter.export_to_archive(
            models=[x.label_lower for x in selected_models],
            add_columns_for='subject_visit_id',
            user=request.user)
        messages.success(
            request, (f'The data for {len(selected_models)} '
                      f'selected models have been sent to your email. '
                      f'({request.user.email})'))

    def get_selected_models_from_post(self):
        exportables = Exportables(request=self.request)
        selected_models = []
        for app_config in exportables:
            selected_models.extend(self.request.POST.getlist(
                f'chk_{app_config.name}_models') or [])
            selected_models.extend(self.request.POST.getlist(
                f'chk_{app_config.name}_historicals') or [])
            selected_models.extend(self.request.POST.getlist(
                f'chk_{app_config.name}_lists') or [])
        return [ModelOptions(model=m).__dict__ for m in selected_models if m]

    def get_selected_models_from_session(self):
        try:
            selected_models = self.request.session.pop('selected_models')
        except KeyError:
            raise NothingToExport('KeyError')
        else:
            if not selected_models:
                raise NothingToExport('Nothing to export')
        return [ModelOptions(**dct) for dct in selected_models]