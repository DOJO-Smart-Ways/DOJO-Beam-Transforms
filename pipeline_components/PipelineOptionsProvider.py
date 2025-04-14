from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions, SetupOptions
import apache_beam as beam
import os
from datetime import datetime
from utils import gcp_utils as gcp
from enums.DataflowMachineType import DataflowMachineType
from enums.BeamRunner import BeamRunner
from enums.DojoBeamTransformVersion import DojoBeamTransformVersion

class PipelineOptionsProvider:
    _gcp_project = None
    _product = None
    _region = None
    _runner = BeamRunner.DIRECT.value
    _template_name = None
    _container_version = DojoBeamTransformVersion.V3_0_0.value
    _extra_package = None
    _machine_type = DataflowMachineType.N1_STANDARD_1.value
    _num_workers = 1
    _max_num_workers = 1
    
    def __init__(self):
        pass

    @property
    def gcp_project(self):
        return self._gcp_project
    
    @gcp_project.setter
    def gcp_project(self, value):
        self._gcp_project = value

    @property
    def product(self):
        return self._product
    
    @product.setter
    def product(self, value):
        self._product = value
    

    @property
    def region(self):
        return self._region
    
    @region.setter
    def region(self, value):
        self._region = value

    @property
    def runner(self):
        return self._runner
    
    @runner.setter
    def runner(self, value):
        BeamRunner.validate(value)
        self._runner = value

    @property
    def template_name(self):
        return self._template_name
    
    @template_name.setter
    def template_name(self, value):
        self._template_name = value

    @property
    def container_version(self):
        return self._container_version
    
    @container_version.setter
    def container_version(self, value):
        DojoBeamTransformVersion.validate(value)
        self._container_version = value

    @property
    def extra_package(self):
        return self._extra_package
    
    @extra_package.setter
    def extra_package(self, value):
        self._extra_package = value

    @property
    def machine_type(self):
        return self._machine_type
    
    @machine_type.setter
    def machine_type(self, value):
        DataflowMachineType.validate(value)
        self._machine_type = value

    @property
    def num_workers(self):
        return self._num_workers
    
    @num_workers.setter
    def num_workers(self, value):
        self._num_workers = value
    
    @property
    def max_num_workers(self):
        return self._max_num_workers
    
    @max_num_workers.setter
    def max_num_workers(self, value):
        self._max_num_workers = value
    
    def getPipelineOptions(self):
        if self.gcp_project is None or self.template_name is None or self.region is None:
            raise ValueError(f'{self.gcp_project} is None or {self.template_name} is None or {self.region} GCP project, template name and region should not be empty or None')
        
        pipeline_options = PipelineOptions(auto_unique_labels=True)

        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.gcp_project
        google_cloud_options.region = self.region
        google_cloud_options.temp_location = gcp.build_gcs_path(f'{self.gcp_project}-temp', 'data-flow-pipelines', 'temp')
        google_cloud_options.staging_location = gcp.build_gcs_path(f'{self.gcp_project}-temp', 'data-flow-pipelines', 'staging')
        
        
        if self.runner == BeamRunner.DATAFLOW.value and self.template_name is None:
            raise ValueError('For DataflowRunner template name is not should be empty or None')
        
        if self.template_name is not None:
            google_cloud_options.template_location = gcp.build_gcs_path(f'{self.gcp_project}-template', self.product, self.template_name)

        pipeline_options.view_as(StandardOptions).runner = self.runner

        
        worker_options = pipeline_options.view_as(WorkerOptions)
        worker_options.sdk_container_image = f'{self.region}-docker.pkg.dev/{self.gcp_project}/dojo-beam/dojo_beam:{self.container_version}'
        worker_options.machine_type = self.machine_type
        worker_options.num_workers = self.num_workers
        worker_options.max_num_workers = self.max_num_workers

        setup_options = pipeline_options.view_as(SetupOptions)
        setup_options.sdk_location = 'container'
        #setup_options.extra_packages = ['gs://nidec-ga-etl/digital-helpchain-dependencies.zip']
        if self.extra_package is not None:
            setup_options.extra_packages = [self.extra_package]

        current_date = datetime.now().strftime('%Y%m%d')
        os.environ['CURRENT_DATE'] = current_date
            
        return beam.Pipeline(options=pipeline_options)