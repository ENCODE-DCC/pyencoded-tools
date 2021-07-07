#######################
# Page object models. #
#######################


class FrontPage:
    """
    Page object models allow selectors to be defined in one place and then
    used by all of the test.
    """
    login_button_css = '#user-actions-footer > a'
    logout_button_text = 'Sign out'
    menu_button_data_css = '#main > ul > li:nth-child(1) > a'
    menu_button_data_alt_css = '#main > ul > li:nth-child(1) > button'
    drop_down_search_button_css = '#main > ul > li.dropdown.open > ul > li:nth-child(2) > a'
    covid_survey_banner_button_css = '#walkme-survey-balloon-69379 > div > div.walkme-custom-balloon-inner-div > div.walkme-custom-balloon-top-div > div > div.walkme-click-and-hover.walkme-custom-balloon-close-button.walkme-action-close.walkme-inspect-ignore'


class SignInModal:
    """
    Page object model.
    """
    login_modal_class = 'auth0-lock-header-logo'
    github_button_css = 'a[data-provider="github"]'
    button_tag = 'button'
    github_user_id_input_css = '#login_field'
    github_password_input_css = '#password'
    github_submit_button_css = '#login > div.auth-form-body.mt-3 > form > div > input.btn.btn-primary.btn-block'
    two_step_user_id_input_css = '#username'
    two_step_password_input_css = '#password'
    two_step_submit_css = '#login > input'
    two_step_send_sms_css = '#sms-send > input.submit-button'
    two_step_code_input_css = '#otp'
    two_step_submit_verification_css = '#otp-box > div > input.go-button'


class SearchPageList:
    """
    Page object model.
    """
    facet_box_class = 'facets'
    facet_class = 'facet'
    facet_expander_button = 'facet__expander--header'
    category_title_class = 'facet-term__text'
    number_class = 'facet-term__count'
    category_title_class_radio = 'facet__radio-label'
    number_class_radio = 'facet__radio-count'
    download_metadata_button_xpath = '//*[contains(text(), "Download")]'
    search_result_item = '(//div[@class="result-item__data"])[1]//a'


class SearchPageMatrix:
    """
    Page object model.
    """
    facet_box_class = 'facets.matrix-facets'
    facets_left_class = 'facet'


class SearchPageSummary:
    """
    Page object model.
    """
    facet_box_class = 'facets.summary-facets'
    facets_left_class = 'facet'


class ExperimentPage:
    """
    Page object model.
    """
    done_panel_class = 'done'
    title_tag_name = 'h4'
    graph_close_button_css = 'div > div:nth-child(2) > div.file-gallery-graph-header.collapsing-title > button'
    sort_by_accession_xpath = '//*[@id="tables"]/div/div[2]/div[2]/div/table[2]/thead/tr[2]/th[1]/div'
    all_buttons_tag_name = 'button'
    download_graph_png_button_xpath = '//*[contains(text(), "Download Graph")]'
    file_type_column_xpath = '//div[@class="file-gallery-counts"]//..//table[@class="table table-sortable"]//tr//td[2]'
    accession_column_relative_xpath = '..//td[1]//span//div//span//a'
    information_button_relative_xpath = '..//td[1]//span//button//i'
    file_graph_tab_xpath = '//div[@class="tab-nav"]//li[2]'
    file_table_tab_xpath = '//a[text()="File details"]'
    assembly_selector_xpath = '//*[@id="tables"]/div/div[1]/div[1]/select[@class="form-control--select"]'
    file_graph_id = 'pipeline-graph'
    incl_deprecated_files_button_name = 'filterIncArchive'
    walkme_corner_widget = '#walkme-player'


class FilePage:
    """
    Page object model.
    """
    download_button_xpath = '//*[contains(text(), "Download")]'


class AntibodyPage:
    """
    Page object model.
    """
    expanded_document_panels_xpath = '//div[@class="document__detail active"]//a[@href]'


class ReportPage:
    """
    Page object model.
    """
    download_tsv_report_button_xpath = '//*[contains(text(), "Download TSV")]'


class VisualizeModal:
    """
    Page object model.
    """
    modal_class = 'modal-content'
    UCSC_link_partial_link_text = 'UCSC'


class DownloadModal:
    """
    Page object model.
    """
    download_button_xpath = '/html/body/div[2]/div/div/div[1]/div/div/div[3]/div/a[2]'


class InformationModal:
    """
    Page object model.
    """
    download_icon_xpath = '//div[@class="modal-body"]//i[@class="icon icon-download"]'


class NavBar:
    """
    Page object model.
    """
    testing_warning_banner_button_css = '#navbar > div.test-warning > div > button'


class LoadingSpinner:
    """
    Page object model.
    """
    loading_spinner_class = 'loading-spinner'


class DocumentPreview:
    """
    Page object model.
    """
    document_expand_buttons_class = 'document__file-detail-switch'
    document_files_xpath = '//div[@class="document__file"]//a[@href]'


class UCSCGenomeBrowser:
    """
    Page object model.
    """
    zoom_one_id = 'hgt.in1'
