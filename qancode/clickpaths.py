import time
import os
import urllib

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support import select

from .pageobjects import (AntibodyPage,
                          DocumentPreview,
                          DownloadModal,
                          ExperimentPage,
                          FilePage,
                          FrontPage,
                          InformationModal,
                          LoadingSpinner,
                          NavBar,
                          ReportPage,
                          SearchPageList,
                          SearchPageMatrix,
                          SignInModal,
                          UCSCGenomeBrowser,
                          VisualizeModal)
    
################
# Click paths. #
################

# --- Genome browsers. ---


class OpenUCSCGenomeBrowser:
    """
    Defines clicks required to open UCSC trackhub from Search page for
    given assembly.
    """

    def __init__(self, driver, assembly):
        self.driver = driver
        self.assembly = assembly
        self.perform_action()

    def perform_action(self):
        current_window = self.driver.current_window_handle
        time.sleep(1)
        for y in self.driver.find_elements_by_tag_name(ExperimentPage.all_buttons_tag_name):
            try:
                if y.text == 'Visualize':
                    y.click()
                    self.driver.wait.until(EC.element_to_be_clickable(
                        (By.CLASS_NAME, VisualizeModal.modal_class)))
                    break
            except:
                pass
        modal = self.driver.wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, VisualizeModal.modal_class)))
        UCSC_links = modal.find_elements_by_partial_link_text(
            VisualizeModal.UCSC_link_partial_link_text)
        for link in UCSC_links:
            if self.assembly in link.get_attribute("href"):
                print('Opening genome browser')
                link.click()
                break
        time.sleep(1)
        self.driver.switch_to_window([h for h in self.driver.window_handles
                                      if h != current_window][0])
        self.driver.wait.until(EC.element_to_be_clickable(
            (By.ID, UCSCGenomeBrowser.zoom_one_id)))
        time.sleep(3)

class OpenUCSCGenomeBrowserFromExperiment:
    """
    Defines clicks required to open UCSC trackhub from Experiment page for
    given assembly.
    """

    def __init__(self, driver, assembly):
        self.driver = driver
        self.assembly = assembly
        self.perform_action()

    def perform_action(self):
        current_window = self.driver.current_window_handle
        time.sleep(1)

        file_table_tab = self.driver.find_element_by_xpath(ExperimentPage.file_table_tab_xpath)
        file_table_tab.click()

        try:
            selector_elem = self.driver.find_element_by_xpath(ExperimentPage.assembly_selector_xpath)
            selector = select.Select(selector_elem)
        except Exception as e:
            print(e)
            exit()

        selected_assembly = self.driver.find_element_by_xpath(
            '//*[@id="tables"]/div/div[1]/div[1]/select/option[starts-with(text(), "{0}")]'.format(self.assembly))

        # Forces driver to scroll to the Assembly selector.
        # Forced scrolling is necessary for the Edge webdriver, otherwise it's unable to interact with the selector.
        try:
            self.driver.execute_script("arguments[0].scrollIntoView(false);", selector_elem);
            selector.select_by_visible_text(selected_assembly.text)
        except Exception as e:
            print(e)
        time.sleep(1)

        for y in self.driver.find_elements_by_tag_name(ExperimentPage.all_buttons_tag_name):
            try:
                if y.text == 'Visualize':
                    y.click()
                    print('Opening genome browser')
                    break
            except:
                pass
        time.sleep(1)
        self.driver.switch_to_window([h for h in self.driver.window_handles
                                      if h != current_window][0])
        time.sleep(3)
        self.driver.wait.until(EC.element_to_be_clickable(
            (By.ID, UCSCGenomeBrowser.zoom_one_id)))
        time.sleep(3)

class OpenUCSCGenomeBrowserGRCh38:
    """
    Opens UCSC browser with GRCh38 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'hg38')


class OpenUCSCGenomeBrowserHG19:
    """
    Opens UCSC browser with hg19 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'hg19')


class OpenUCSCGenomeBrowserMM9:
    """
    Opens UCSC browser with mm9 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'mm9')


class OpenUCSCGenomeBrowserMM10:
    """
    Opens UCSC browser with mm10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'mm10')


class OpenUCSCGenomeBrowserMM10Minimal:
    """
    Opens UCSC browser with mm10-minimal assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'mm10')


class OpenUCSCGenomeBrowserDM3:
    """
    Opens UCSC browser with dm3 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'dm3')


class OpenUCSCGenomeBrowserDM6:
    """
    Opens UCSC browser with dm6 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'dm6')


class OpenUCSCGenomeBrowserCE10:
    """
    Opens UCSC browser with ce10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'ce10')


class OpenUCSCGenomeBrowserCE11:
    """
    Opens UCSC browser with ce11 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowser(driver, 'ce11')

# --- Classes for opening from the Experiment page ---

class OpenUCSCGenomeBrowserGRCh38fromExperiment:
    """
    Opens UCSC browser with GRCh38 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'GRCh38')


class OpenUCSCGenomeBrowserHG19fromExperiment:
    """
    Opens UCSC browser with hg19 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'hg19')


class OpenUCSCGenomeBrowserMM9fromExperiment:
    """
    Opens UCSC browser with mm9 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'mm9')


class OpenUCSCGenomeBrowserMM10fromExperiment:
    """
    Opens UCSC browser with mm10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'mm10')


class OpenUCSCGenomeBrowserMM10MinimalfromExperiment:
    """
    Opens UCSC browser with mm10-minimal assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'mm10')


class OpenUCSCGenomeBrowserDM3fromExperiment:
    """
    Opens UCSC browser with dm3 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'dm3')


class OpenUCSCGenomeBrowserDM6fromExperiment:
    """
    Opens UCSC browser with dm6 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'dm6')


class OpenUCSCGenomeBrowserCE10fromExperiment:
    """
    Opens UCSC browser with ce10 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'ce10')


class OpenUCSCGenomeBrowserCE11fromExperiment:
    """
    Opens UCSC browser with ce11 assembly.
    """

    def __init__(self, driver):
        OpenUCSCGenomeBrowserFromExperiment(driver, 'ce11')


# --- File downloads. ---

class DownloadFileFromTable:
    """
    Download specified filetype from file table.
    """

    def __init__(self, driver, filetype):
        self.driver = driver
        self.filetype = filetype

    def perform_action(self):
        filenames = []
        download_start_times = []
        # Get all links on page.
        elems = self.driver.wait.until(
            EC.presence_of_all_elements_located((By.XPATH, '//span/a')))
        for elem in elems:
            # Find and click on first link with filetype.
            if self.filetype in elem.get_attribute('href'):
                filename = urllib.request.unquote(elem.get_attribute('href').split(
                    '/')[-1])
                download_start_time = time.time()
                elem.click()
                print('Downloading {} from {}'.format(
                    filename, elem.get_attribute('href')))
                filenames.append(filename)
                download_start_times.append(download_start_time)
                time.sleep(2)
                break
        else:
            raise ValueError('{}WARNING: File not found{}'.format(
                '\x1b[31m', '\x1b[0m'))
        return filenames, download_start_times

class DownloadBEDFileFromTable:
    """
    Download bed.gz file from file table.
    """

    def __init__(self, driver):
        self.filenames, self.download_start_times = DownloadFileFromTable(
            driver, 'bed.gz').perform_action()


class DownloadFileFromModal:
    """
    Download file from information modal on file table.
    """

    def __init__(self, driver, full_file_type):
        self.driver = driver
        self.full_file_type = full_file_type

    def perform_action(self):
        elems = self.driver.find_elements_by_xpath(
            ExperimentPage.file_type_column_xpath)
        for elem in elems:
            if elem.text == self.full_file_type:
                filename = urllib.request.unquote(elem.find_element_by_xpath(
                    ExperimentPage.accession_column_relative_xpath).get_attribute('href').split('/')[-1])
                download_start_time = time.time()
                elem.find_element_by_xpath(
                    ExperimentPage.information_button_relative_xpath).click()
                break
        time.sleep(2)
        self.driver.find_element_by_xpath(
            InformationModal.download_icon_xpath).click()
        time.sleep(5)
        print(filename)
        return [filename], [download_start_time]


class DownloadBEDFileFromModal:
    """
    Download bed narrowPeak file from information modal.
    """

    def __init__(self, driver):
        self.filenames, self.download_start_times = DownloadFileFromModal(
            driver, 'bed narrowPeak').perform_action()


class DownloadFileFromButton:
    """
    Download file based on button text.
    """

    def __init__(self, driver, button_xpath, filename):
        self.driver = driver
        self.button_xpath = button_xpath
        self.filename = filename

    def perform_action(self):
        button = self.driver.wait.until(EC.element_to_be_clickable(
            (By.XPATH, self.button_xpath)))
        filenames = [self.filename]
        download_start_times = [time.time()]
        button.click()
        time.sleep(5)
        return filenames, download_start_times


class DownloadGraphFromExperimentPage:
    """
    Download file graph from Experiment page.
    """

    def __init__(self, driver):
        # Filename is accession.png.
        self.filenames, self.download_start_times = DownloadFileFromButton(
            driver, ExperimentPage.download_graph_png_button_xpath,
            '{}.png'.format(driver.current_url.split('/')[-2])).perform_action()


class DownloadTSVFromReportPage:
    def __init__(self, driver):
        self.filenames, self.download_start_times = DownloadFileFromButton(
            driver, ReportPage.download_tsv_report_button_xpath, 'Report.tsv').perform_action()


class DownloadMetaDataFromSearchPage:
    def __init__(self, driver):
        # Get rid of any files.txt from Downloads.
        download_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        for file in os.listdir(download_folder):
            if ((file.startswith('files') and file.endswith('.txt'))
                    or (file.startswith('metadata') and file.endswith('.tsv'))):
                print('Deleting old files.txt/metadata.tsv')
                try:
                    os.remove(os.path.join(download_folder, file))
                except:
                    pass
        # Click on page.
        DownloadFileFromButton(
            driver, SearchPageList.download_metadata_button_xpath, None).perform_action()
        # Click on modal.
        self.filenames, self.download_start_times = DownloadFileFromButton(
            driver, DownloadModal.download_button_xpath, 'files.txt').perform_action()


class DownloadFileFromFilePage:
    def __init__(self, driver):
        self.filenames, self.download_start_times = DownloadFileFromButton(driver, FilePage.download_button_xpath, driver.find_element_by_xpath(
            FilePage.download_button_xpath).get_attribute('href').split('/')[-1]).perform_action()


class DownloadDocuments:
    """
    Download all files from documents panel (except Antibody pages).
    """

    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        self.filenames = []
        self.download_start_times = []
        elems = self.driver.find_elements_by_xpath(
            DocumentPreview.document_files_xpath)
        for elem in elems:
            filename = urllib.request.unquote(
                elem.get_attribute('href').split('/')[-1])
            download_start_time = time.time()
            elem.click()
            print('Downloading {} from {}'.format(
                filename, elem.get_attribute('href')))
            self.filenames.append(filename)
            self.download_start_times.append(download_start_time)
            time.sleep(2)


class DownloadDocumentsFromAntibodyPage:
    """
    Download all files from document panel on Antibody page.
    """

    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        self.filenames = []
        self.download_start_times = []
        elems = self.driver.find_elements_by_xpath(
            AntibodyPage.expanded_document_panels_xpath)
        for elem in elems:
            key = elem.get_attribute('href')
            # Filter out non-files.
            if not key.endswith('/'):
                filename = urllib.request.unquote(key.split('/')[-1])
                download_start_time = time.time()
                try:
                     # Safari bug workaround to download PDFs from Antibody page.
                    if ((self.driver.capabilities['browserName'] == 'safari')
                            and key.endswith('pdf')):
                        self.driver.execute_script(
                            'arguments[0].click();', elem)
                    else:
                        elem.click()
                except:
                    print('Error clicking on {}'.format(key))
                    continue
                print('Downloading {} from {}'.format(filename, key))
                self.filenames.append(filename)
                self.download_start_times.append(download_start_time)
                time.sleep(2)


class ClickSearchResultItem:
    """
    Clicks the first item on a page of search results.
    """

    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        try:
            item_link = self.driver.find_element_by_xpath(SearchPageList.search_result_item)
            self.driver.execute_script('arguments[0].scrollIntoView(true)', item_link)
            self.driver.execute_script('arguments[0].click()', item_link)
        except:
            pass


class ClickSearchResultItemAndMakeExperimentPagesLookTheSame:
    """
    Clicks the first item on a page of search results.
    """

    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        try:
            item_link = self.driver.find_element_by_xpath(SearchPageList.search_result_item)
            self.driver.execute_script('arguments[0].scrollIntoView(true)', item_link)
            self.driver.execute_script('arguments[0].click()', item_link)
            self.driver.wait.until(
                EC.element_to_be_clickable((By.XPATH, ExperimentPage.file_table_tab_xpath))
            ).click()
            self.driver.wait.until(
                EC.element_to_be_clickable((By.XPATH, ExperimentPage.sort_by_accession_xpath))
            ).click()
        except:
            pass


class MakeExperimentPagesLookTheSameByClickingFileTab:
    """
    Makes two experiment pages look the same by clicking on the file tab and sorting by accession.
    """

    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        try:
            self.driver.wait.until(
                EC.element_to_be_clickable((By.XPATH, ExperimentPage.file_table_tab_xpath))
            ).click()
            self.driver.wait.until(
                EC.element_to_be_clickable((By.XPATH, ExperimentPage.sort_by_accession_xpath))
            ).click()
        except:
            pass


class MakeExperimentPagesLookTheSameByHidingGraph:
    """
    Makes two experiment pages look the same by making hiding the file graph. The "include
    deprecated files" button is also checked in order to get the file status bar to appear.
    """
    def __init__(self, driver):
        self.driver = driver
        self.perform_action()

    def perform_action(self):
        try:
            file_graph = self.driver.wait_long.until(
                EC.presence_of_element_located((By.ID, ExperimentPage.file_graph_id))
            )
            self.driver.execute_script("arguments[0].style.visibility='hidden'", file_graph)
            checkbox = self.driver.wait.until(
                EC.element_to_be_clickable(
                    (By.NAME, ExperimentPage.incl_deprecated_files_button_name)
                )
            )
            if not checkbox.is_selected():
                checkbox.click()
        except:
            pass
