from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions, SetupOptions
import apache_beam as beam
import os
from datetime import datetime
from utils import gcp_utils as gcp
from pipelines.commons.DataflowMachineType import DataflowMachineType
from pipelines.commons.BeamRunner import BeamRunner
from pipelines.commons.DojoBeamTransformVersion import DojoBeamTransformVersion

class PipelineOptionsProvider:
    _project = os.getenv('PROJECT')
    _region = os.getenv('REGION')
    _runner = BeamRunner.DIRECT.value
    _template_name = None
    _container_version = DojoBeamTransformVersion.V1_0_0.value
    _extra_package = None
    _machine_type = DataflowMachineType.N1_STANDARD_1.value
    
    def __init__(self):
        pass

    @property
    def project(self):
        return self._project
    
    @project.setter
    def project(self, value):
        self._project = value

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
    
    def getPipelineOptions(self):
        pipeline_options = PipelineOptions(auto_unique_labels=True)

        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.project
        google_cloud_options.region = self.region
        google_cloud_options.temp_location = gcp.build_gcs_path(f'{self.project}-temp', 'data-flow-pipelines', 'temp')
        google_cloud_options.staging_location = gcp.build_gcs_path(f'{self.project}-temp', 'data-flow-pipelines', 'staging')
        
        
        if self.runner == BeamRunner.DATAFLOW.value and self.template_name is None:
            raise ValueError('For DataflowRunner template name is not should be empty or None')
        
        if self.template_name is not None:
            google_cloud_options.template_location = gcp.build_gcs_path(f'{self.project}-temp', 'data-flow-pipelines', 'template', self.template_name)

        pipeline_options.view_as(StandardOptions).runner = self.runner

        
        worker_options = pipeline_options.view_as(WorkerOptions)
        #worker_options.worker_harness_container_image = f'{self.region}-docker.pkg.dev/{self.project}/dojo-beam/dojo_beam:1.0.0'
        worker_options.worker_harness_container_image = f'{self.region}-docker.pkg.dev/{self.project}/dojo-beam/dojo_beam:{self.container_version}'

        worker_options.machine_type = self.machine_type

        setup_options = pipeline_options.view_as(SetupOptions)
        setup_options.sdk_location = 'container'
        #setup_options.extra_packages = ['gs://nidec-ga-etl/digital-helpchain-dependencies.zip']
        if self.extra_package is not None:
            setup_options.extra_packages = [self.extra_package]

        current_date = datetime.now().strftime('%Y%m%d')
        os.environ['CURRENT_DATE'] = current_date
            
        return beam.Pipeline(options=pipeline_options)