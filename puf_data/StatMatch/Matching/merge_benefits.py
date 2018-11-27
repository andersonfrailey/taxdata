"""
Merge imputed benefits from the open source C-TAM model to the CPS file
"""
import pandas as pd


def benefit_merge(cps):
    """"
    Merge benefit variables from C-TAM onto the given CPS file
    """

    # read in individual benefit files
    print('Reading imputed benefits')
    housing = pd.read_csv('data/Housing_Imputation_logreg_spm.csv')
    snap = pd.read_csv('data/SNAP_Imputation.csv')
    ssi = pd.read_csv('data/SSI_Imputation.csv',
                      usecols=['peridnum', 'ssi_participation', 'ssi_impute',
                               'probs'])
    ssi.rename({'probs': 'ssi_probs'}, inplace=True)
    tanf = pd.read_csv('data/TANF_Imputation.csv')
    tanf.rename({'probs': 'tanf_probs'}, inplace=True)
    wic_children = pd.read_csv('data/WIC_imputation_children.csv', index_col=0)
    wic_children.rename({'WIC_impute': 'wic_impute_children',
                         'WIC_participation': 'wic_participation_children'},
                        axis=1, inplace=True)
    wic_infants = pd.read_csv('data/WIC_imputation_infants.csv', index_col=0)
    wic_infants.rename({'WIC_impute': 'wic_impute_infants',
                        'WIC_participation': 'wic_participation_infants'},
                       axis=1, inplace=True)
    wic_women = pd.read_csv('data/WIC_imputation_women.csv', index_col=0)
    wic_women.rename({'WIC_impute': 'wic_impute_women',
                      'WIC_participation': 'wic_participation_women'},
                     axis=1, inplace=True)

    # merge benefits onto the CPS
    original_len = len(cps)
    print('Merging benefits to CPS')
    cps_merged = cps.merge(housing, on=['fh_seq', 'ffpos'], how='left')
    assert(len(cps_merged) == original_len), 'housing'
    cps_merged = cps_merged.merge(snap, on='h_seq', how='left')
    assert(len(cps_merged) == original_len), 'snap'
    cps_merged = cps_merged.merge(ssi, on='peridnum', how='left')
    assert(len(cps_merged) == original_len), 'ssi'
    # cps_merged = cps_merged.merge(tanf, on='peridnum', how='left')
    # assert(len(cps_merged) == original_len), 'tanf'
    cps_merged = cps_merged.merge(wic_children, on='peridnum', how='left')
    assert(len(cps_merged) == original_len), 'wic children'
    cps_merged = cps_merged.merge(wic_infants, on='peridnum', how='left')
    assert(len(cps_merged) == original_len), 'wic infants'
    cps_merged = cps_merged.merge(wic_women, on='peridnum', how='left')
    assert(len(cps_merged) == original_len), 'wic women'

    return cps_merged


if __name__ == '__main__':
    cps = pd.read_csv('cpsmar2016.csv')
    cps_merged = benefit_merge(cps)
    cps_merged.to_csv('cpsmar2016_aug.csv', index=None)
