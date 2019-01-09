import luigi

import sys
import os
sys.path.append('luigi_scripts')
from tasks import EvaluateCombinations

if __name__ == '__main__':

    # Add here all essential parameters of the pipeline, i.e., parameters that
    # change the output and can not be infered from others. Examples for
    # non-essential parameters are db_host (global settings) and input/output
    # filenames (should be inferred from other parameters like 'experiment' and
    # 'sample'.

    general_parameters = {
        'experiment': 'cremi',
        'setup': [
            'setup01',
            'setup02'
        ],
        'iteration': [300000, 400000],
        'sample': 'testing/sample_C_padded_20160501.aligned.filled.cropped.62:153.n5'
    }

    data_parameters = {
        'candidate_extraction_mode': 'single'
    }

    preprocessing_parameters = {
        'gaussian_sigma_single': [0.1, 0.2]
        # TODO: add others
    }

    output_parameters = {
        'roi_x': (0, 100),
        'roi_y': (0, 100),
        'roi_z': (0, 100)
    }

    solve_parameters = {
        'cc_min_vertices': [4, 5, 6]
        # TODO: add others
    }

    # Range keys map to lists, all the combinations of their elements will
    # create on EvaluateTask
    range_keys = [
        'setup',
        'iteration',
        'gaussian_sigma_single',
        'cc_min_vertices'
    ]

    luigi.build(
            [EvaluateCombinations(
                general_parameters,
                data_parameters,
                preprocessing_parameters,
                output_parameters,
                solve_parameters,
                range_keys)],
            workers=10,
            scheduler_host='slowpoke1.int.janelia.org'
    )

