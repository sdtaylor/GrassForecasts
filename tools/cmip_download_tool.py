from ftplib import FTP
import os
import time
import urllib.request

class CMIP_FTP_TOOL:
    def __init__(self, 
                 host='gdo-dcp.ucllnl.org', 
                 base_url_dir = '/pub/dcp/archive/cmip5/bcca/',
                 user='anonymous', passwd='abc123',
                 verbose=False):
        """
        This queries and allows quick download for the ftp server hosting files at
        https://gdo-dcp.ucllnl.org/.
        
        Specifically the BCCAv2-CMIP5 Climate daily Projections available under
        ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/bcca
        
        Usage:
            
        cmip = CMIP_FTP_TOOL()

        # This downloads 4 files total (2 decades x 2 runs)
        cmip.download_cmip5_file(scenario = 'rcp85',
                                 climate_model='ccsm4',
                                 variable = 'pr',
                                 decades = ['2006','2026'],
                                 n_runs=2,
                                 dest_folder = './data/')

        """
        self.host=host
        self.base_url_dir = base_url_dir
        self.user=user
        self.passwd=passwd
        self._folder_file_lists={}
        self.verbose = verbose
        
        self.connect()
        
        self.available_models = ['canesm2','ccsm4','csiro-mk3-6-0','gfdl-esm2g',
                                 'miroc5','ipsl-cm5a-lr']
        self.available_variabels = ['pr','tasmax','tasmin']
        self.available_scenarios = ['rcp26','rcp45','rcp60','rcp85']

        # The ftp files are split by the following 10 year periods
        #                                    startdate, endate
        self.available_decades = {'2006' : ['20060101','20151231'],
                                  '2016' : ['20160101','20251231'],
                                  '2026' : ['20260101','20351231'],
                                  '2036' : ['20360101','20451231'],
                                  '2046' : ['20460101','20551231'],
                                  '2056' : ['20560101','20651231'],
                                  '2066' : ['20660101','20751231'],
                                  '2076' : ['20760101','20851231'],
                                  '2086' : ['20860101','20951231'],
                                  '2096' : ['20960101','21001231']}
    
    def _query_ftp_folder(self, folder, attempts_made=0):
        connect_attempts=5
        retry_wait_time=300
        try:
            dir_listing = self.con.nlst(folder)
            return dir_listing
        except:
            if attempts_made + 1 == connect_attempts:
                raise IOError('Cannot query ftp')
            else:
                print('Cannot query folder, reconnecting and retrying in {t} sec'.format(t=retry_wait_time))
                time.sleep(retry_wait_time)
                self.close()
                time.sleep(1)
                self.connect()
                return self._query_ftp_folder(folder, attempts_made = attempts_made + 1)
            
    def connect(self):
        self.con = FTP(host=self.host, user=self.user, passwd=self.passwd)

    def close(self):
        self.con.close()

    def _get_folder_listing(self, folder):
        """
        Querying the ftp takes a few moments, so if a folder is queried once,
        save the listing for future reference.
        """
        if folder in self._folder_file_lists:
            return self._folder_file_lists[folder]
        else:
            dir_listing = self._query_ftp_folder(folder)
            self._folder_file_lists[folder]=dir_listing
            return dir_listing
        
    def _download_file(self, download_path, dest_path, num_attempts=2):
        """
        Perform the actual download for a single file, with multiple
        tries if the connection/server is spotty.
        """
        for attempt in range(1,num_attempts+1):
            try:
                urllib.request.urlretrieve(download_path, dest_path)
            except:
                if attempt==num_attempts:
                    raise
                else:
                    time.sleep(30)
                    continue

    def _get_n_run_names(self, n_runs):
        return ['r{n}i1p1'.format(n=n) for n in range(1,n_runs+1)]
    
    def download_cmip5_file(self, scenario, climate_model, variable, decades, n_runs, dest_folder):
        """
        Download this combo of stuff with at most n_runs. Will raise a
        RuntimeError if a specific combination isn't available. 


        Parameters
        ----------
        scenario : str
            one of 'rcp26','rcp45','rcp60','rcp85'. Not all scenarios are available
            for all climate models. 
        climate_model : str
            see available climate models in __init__.
        variable : str
            variable to get, one of ['pr','tasmax','tasmin'].
        decades : list of strs
            list of timeperiods to get. see available decades in __init__.
        n_runs : int
            the  max number of runs to get. some models/scenarios have up to 10, but
            most only 1-3
        dest_folder : str
            destination folder to download to

        Raises
        ------
        RuntimeError
            If a model/scenario isn't available.

        Returns
        -------
        None.

        """
        # is this scenario+model available ?
        model_folder = '/{b}/{m}/{s}/day/'.format(b=self.base_url_dir,
                                                  m=climate_model,
                                                  s=scenario)
        
        # get avilable runs. if this folder is empty the comboination isn't available
        run_listing = self._get_folder_listing(model_folder)
        if len(run_listing)==0:
            raise RuntimeError('Scenario/model combo not available')
        
        available_runs = [os.path.basename(r) for r in run_listing]
        
        requested_runs = self._get_n_run_names(n_runs)
        
        runs_to_try = [r for r in requested_runs if r in available_runs]
        
        if(len(runs_to_try) < len(requested_runs)):
            msg='Only {n1} runs available, requested {n2}'.format(n1=len(runs_to_try),
                                                                  n2=n_runs)
            print(msg)
            
        for r in runs_to_try:
            for d in decades:
                download_url = self.build_cmip5_url(scenario, climate_model, variable, d, r)
                self._download_file(download_url, dest_path = dest_folder + os.path.basename(download_url))
                
        
    
    def build_cmip5_url(self, scenario, climate_model, variable, decade, run):
        """
        Turn the different components into:
        ftp://gdo-dcp.ucllnl.org/pub/dcp/archive/cmip5/bcca/ccsm4/rcp85/day/r1i1p1/pr/BCCAv2_0.125deg_pr_day_CCSM4_rcp85_r1i1p1_20060101-20151231.nc4
        """
        # Precip got an update due to an error, thus has a different prefix
        if variable == 'pr':
            file_prefix = 'BCCAv2_0'
        else:
            file_prefix = 'BCCA_0'
            
        decade_start = self.available_decades[decade][0]
        decade_end   = self.available_decades[decade][1]
        filename = '{p}.125deg_{v}_day_{m}_{s}_{r}_{d1}-{d2}.nc4'.format(p=file_prefix,
                                                                         v = variable,
                                                                         m = climate_model.upper(),
                                                                         s = scenario,
                                                                         r = run,
                                                                         d1= decade_start,
                                                                         d2= decade_end)
        
        return 'ftp://{h}/{b}/{m}/{s}/day/{r}/{v}/{f}'.format(h=self.host,
                                                              b=self.base_url_dir,
                                                              m=climate_model,
                                                              s=scenario,
                                                              r=run,
                                                              v=variable,
                                                              f=filename)

    def cmip5_file_available(self, cmip5_path):
        """
        A quick check to see if a file is actually there, returns bool
        """
        #TODO
        pass








