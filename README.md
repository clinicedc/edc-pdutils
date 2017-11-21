# edc-pdutils

Use pandas with the Edc


To export Crf data, for example:

    csv_path = '/Users/erikvw/Documents/ambition/export/'
    date_format = '%Y-%m-%d'
    sep = ','

    class MyDfHandler(CrfDfHandler):
        visit_tbl = 'ambition_subject_subjectvisit'
        registered_subject_tbl = 'edc_registration_registeredsubject'
        appointment_tbl = 'edc_appointment_appointment'
        exclude_columns = ['form_as_json', 'survival_status','last_alive_date',
                           'screening_age_in_years', 'registration_datetime',
                           'subject_type']
    
    class MyCsvCrfTablesExporter(CsvCrfTablesExporter):
        visit_columns = ['subject_visit_id']
        datetime_fields = ['randomization_datetime']
        df_handler_cls = MyDfHandler
        app_label = 'ambition_subject'
        export_folder = csv_path
    
    sys.stdout.write('\n')
    exporter = MyCsvCrfTablesExporter()
    exporter.to_csv(date_format=date_format, delimiter=sep)
    
To export INLINE data for any CRF configured with an inline, for example:

    class MyDfHandler(CrfDfHandler):
        visit_tbl = 'ambition_subject_subjectvisit'
        registered_subject_tbl = 'edc_registration_registeredsubject'
        appointment_tbl = 'edc_appointment_appointment'
        exclude_columns = ['form_as_json', 'survival_status','last_alive_date',
                           'screening_age_in_years', 'registration_datetime',
                           'subject_type']
    
    
    class MyCsvCrfInlineTablesExporter(CsvCrfInlineTablesExporter):
        visit_columns = ['subject_visit_id']
        df_handler_cls = MyDfHandler
        app_label = 'ambition_subject'
        export_folder = csv_path
        exclude_inline_tables = [
            'ambition_subject_radiology_abnormal_results_reason',
            'ambition_subject_radiology_cxr_type']
    sys.stdout.write('\n')
    exporter = MyCsvCrfInlineTablesExporter()
    exporter.to_csv(date_format=date_format, delimiter=sep)
