import luigi
import os
import itertools
import hashlib
from targets import *
from daisy.processes import call

# These scripts assume the following directory layout:
#
#   mtrack_experiments (this repository)
#   |
#   |-  <experiment dirs> (e.g., 'cremi', 'l3', 'fib25')
#   |   |
#   |   |-  <data_dir>
#   |   |-  <train_dir>
#   |   \-  <predict_dir>
#   |
#   \-  scripts
#       |
#       \-  luigi_scripts
#           |
#           |-  targets.py
#           \-  tasks.py (this file)

# names of directories, relative to experiment dir
data_dir = '01_data'
train_dir = '02_setups'
predict_dir = '03_predict'

# the MongoDB host to use
db_host = '10.40.4.51'

# where to find the experiment directories:
this_dir = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(this_dir, '..', '..')


def set_base_dir(d):
    global base_dir
    base_dir = d


def mkdirs(path):
    '''Little helper that doesn't fail if the directories already exist.'''
    try:
        os.makedirs(path)
    except:
        pass


class MtrackTask(luigi.Task):
    '''General prarameters and methods for each mtrack task.'''

    # name of experiment
    experiment = luigi.Parameter()

    # which CNN to use
    setup = luigi.Parameter()

    # at which iteration
    iteration = luigi.IntParameter()

    # name of the sample to process
    sample = luigi.Parameter()

    # dictionaries of "essential" parameters
    data_parameters = luigi.DictParameter()
    preprocessing_parameters = luigi.DictParameter()
    output_parameters = luigi.DictParameter()
    solve_parameters = luigi.DictParameter()

    def input_data_dir(self):
        return os.path.join(
            base_dir,
            self.experiment,
            data_dir)

    def train_dir(self):
        return os.path.join(
            base_dir,
            self.experiment,
            train_dir,
            self.setup)

    def predict_dir(self):
        return os.path.join(
            base_dir,
            self.experiment,
            predict_dir,
            self.setup,
            str(self.iteration))

    def db_name(self):

        return '_'.join([
            self.experiment,
            self.setup,
            str(int(self.iteration/1000)) + 'k'
        ])

    def config_hash(self):
        config_str = repr(sorted(self.to_str_params())).encode()
        return hashlib.sha1(config_str).hexdigest()

class EvaluateTask(MtrackTask):

    def requires(self):
        return TrackTask(
            self.experiment,
            self.setup,
            self.iteration,
            self.sample,
            self.data_parameters,
            self.preprocessing_parameters,
            self.output_parameters,
            self.solve_parameters)

    def run(self):

        log_out = os.path.join(self.output_basename(), '.evaluate.out')
        log_err = os.path.join(self.output_basename(), '.evaluate.err')

        call([
                'run_lsf',
                '-c', '5',
                'python', '...' # TODO: add call to evaluation code
            ],
            log_out=log_out,
            log_err=log_err)

    def output(self):
        return MongoDbDocumentTarget(
            self.db_name(),
            db_host,
            # TODO: replace collection name?
            'results',
            # TODO: provide partial document to check for existence
            {})

    def output_basename(self):

        return os.path.join(
            self.predict_dir(),
            self.sample,
            self.config_hash())

class TrackTask(MtrackTask):

    def requires(self):
        # TODO: return PredictTask if implemented
        return []

    def run(self):

        log_out = os.path.join(self.output_basename(), 'track.out')
        log_err = os.path.join(self.output_basename(), 'track.err')

        mkdirs(self.output_basename())

        mtrack.mt_utils.gen_config(
            # TODO: add non-essential parameters
            **self.data_parameters,
            **self.preprocessing_parameters,
            **self.output_parameters,
            **self.solve_parameters,
            cfg_output_dir=self.output_basename())

        call([
                'run_lsf',
                '-c', '5',
                'python', '...', # TODO: add call to track code
                os.path.join(self.output_basename(), 'config.ini')
            ],
            log_out=log_out,
            log_err=log_err)

    def output(self):
        return MongoDbCollectionTarget(
            self.db_name(),
            db_host,
            # TODO: other collection name?
            self.config_hash())

    def output_basename(self):

        return os.path.join(
            self.predict_dir(),
            self.sample,
            self.config_hash())


class EvaluateCombinations(luigi.task.WrapperTask):

    general_parameters = luigi.DictParameter()
    data_parameters = luigi.DictParameter()
    preprocessing_parameters = luigi.DictParameter()
    output_parameters = luigi.DictParameter()
    solve_parameters = luigi.DictParameter()

    # names of parameters that should be exploded
    range_keys = luigi.ListParameter()

    def requires(self):

        general_parameters = self.explode(self.general_parameters)
        data_parameters = self.explode(self.data_parameters)
        preprocessing_parameters = self.explode(self.preprocessing_parameters)
        output_parameters = self.explode(self.output_parameters)
        solve_parameters = self.explode(self.solve_parameters)

        tasks = []
        for (g, d, p, o, s) in itertools.product(
                general_parameters,
                data_parameters,
                preprocessing_parameters,
                output_parameters,
                solve_parameters):

            tasks.append(
                EvaluateTask(
                    **g,
                    data_parameters=d,
                    preprocessing_parameters=p,
                    output_parameters=o,
                    solve_parameters=s
                )
            )

        return tasks

    def explode(self, parameters):

        # get all the values to explode
        range_values = {
            k: v
            for k, v in parameters.items()
            if k in self.range_keys
        }

        other_values = {
            k: v
            for k, v in parameters.items()
            if k not in self.range_keys
        }

        range_keys = range_values.keys()
        exploded_parameters = []

        for concrete_values in itertools.product(*list(range_values.values())):

            exploded = { k: v for k, v in zip(range_keys, concrete_values) }
            exploded.update(other_values)

            exploded_parameters.append(exploded)

        return exploded_parameters
