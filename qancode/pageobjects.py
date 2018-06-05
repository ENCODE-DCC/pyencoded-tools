#######################
# Page object models. #
#######################


class FrontPage:
    """
    Page object models allow selectors to be defined in one place and then
    used by all of the test.
    """
    login_button_text = 'Submitter sign-in'
    logout_button_text = 'Submitter sign out'
    menu_button_data_css = '#main > ul > li:nth-child(1) > a'
    menu_button_data_alt_css = '#main > ul > li:nth-child(1) > button'
    drop_down_search_button_css = '#main > ul > li.dropdown.open > ul > li:nth-child(2) > a'


class SignInModal:
    """
    Page object model.
    """
    login_modal_class = 'auth0-lock-header-logo'
    google_button_css = '#auth0-lock-container-1 > div > div.auth0-lock-center > form > div > div > div:nth-child(3) > span > div > div > div > div > div > div > div > div > div > div.auth0-lock-social-buttons-container > button:nth-child(2) > div.auth0-lock-social-button-icon'
    button_tag = 'button'
    user_id_input_css = '#identifierId'
    user_next_button_css = '#identifierNext > content > span'
    password_input_css = '#password > div.aCsJod.oJeWuf > div > div.Xb9hP > input'
    password_next_button_css = '#passwordNext > content > span'
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
    see_more_buttons_css = '#content > div > div > div > div > div.col-sm-5.col-md-4.col-lg-3 > div > div > div > ul > div.pull-right > small > button'
    facet_class = 'facet'
    category_title_class = 'facet-term__text'
    number_class = 'facet-term__count'
    download_metadata_button_xpath = '//*[contains(text(), "Download")]'


class SearchPageMatrix:
    """
    Page object model.
    """
    see_more_top_buttons_css = '#content > div > div > div > div.col-sm-7.col-md-8.col-lg-9.sm-no-padding > div > div > div > ul > div.pull-right > small > button'
    see_more_left_buttons_css = '#content > div > div > div > div.col-sm-5.col-md-4.col-lg-3.sm-no-padding > div > div > div > ul > div.pull-right > small > button'
    box_top_css = '#content > div > div > div:nth-child(1) > div.col-sm-7.col-md-8.col-lg-9.sm-no-padding > div'
    box_left_css = '#content > div > div > div:nth-child(2) > div.col-sm-5.col-md-4.col-lg-3.sm-no-padding > div'
    facets_top_class = 'facet'
    facets_left_class = 'facet'


class ExperimentPage:
    """
    Page object model.
    """
    done_panel_class = 'done'
    title_tag_name = 'h4'
    graph_close_button_css = 'div > div:nth-child(2) > div.file-gallery-graph-header.collapsing-title > button'
    sort_by_accession_xpath = '//div[@class="file-gallery-counts"]//..//table[@class="table table-sortable"]/thead/tr[2]/th[1]/span/i'
    all_buttons_tag_name = 'button'
    download_graph_png_button_xpath = '//*[contains(text(), "Download Graph")]'
    file_type_column_xpath = '//div[@class="file-gallery-counts"]//..//table[@class="table table-sortable"]//tr//td[2]'
    accession_column_relative_xpath = '..//td[1]//span//div//span//a'
    information_button_relative_xpath = '..//td[1]//span//button//i'
    file_graph_tab_xpath = '//div[@class="tab-nav"]//li[2]'


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
    download_icon_xpath = '/html/body/div[2]/div/div/div[1]/div/div/div[2]/div/dl/div[8]/dd/span/div/span/a/i'


class NavBar:
    """
    Page object model.
    """
    testing_warning_banner_button_css = '#navbar > div.test-warning > div > p > button'


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
