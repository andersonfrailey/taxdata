import cpsmar
import pandas as pd
import os
from adj_filst import adjfilst
from cps_rets import Returns
from soi_rets import create_soi
from phase1 import phaseone
from phase2 import phasetwo
from add_cps_vars import add_cps
from add_nonfilers import add_nonfiler
from merge_benefits import benefit_merge

"""
Script to run each phase of the matching process
"""


def match():
    # If there is a .CSV version of the CPS, simply read that in. Otherwise
    # convert the .DAT file to a .CSV
    cps_csv_path = 'cpsmar2016_aug.csv'
    if os.path.isfile(cps_csv_path):
        print('Reading CPS Data from .CSV')
        mar_cps = pd.read_csv(cps_csv_path)
    else:
        raw_cps_path = 'cpsmar2016.csv'
        cps_dat_path = 'asec2016_pubuse_v3.dat'
        if os.path.isfile(raw_cps_path):
            print('Merging Benefit Data')
            raw_cps = pd.read_csv(raw_cps_path)
            mar_cps = benefit_merge(raw_cps)
            mar_cps.to_csv('cpsmar2016_aug.csv', index=None)
        elif os.path.isfile(cps_dat_path):
            print('Converting .DAT to .CSV')
            raw_cps = cpsmar.create_cps(cps_dat_path)
            mar_cps = benefit_merge(raw_cps)
            mar_cps.to_csv(cps_csv_path, index=None)
        else:
            m = ('You must have either the .DAT or .CSV version of the 2016' +
                 ' CPS in your directory')
            raise FileNotFoundError(m)
    print('Reading PUF Data')
    puf_path = 'puf2011.csv'
    puf = pd.read_csv(puf_path)
    # Change PUF columns to lowercase
    puf.columns = map(str.lower, puf.columns)
    # Remove aggregated variables from the PUF
    puf = puf[(puf['recid'] != 999996) & (puf['recid'] != 999997) &
              (puf['recid'] != 999998) & (puf['recid'] != 999999)]

    print('Creating CPS Tax Units')
    rets = Returns(mar_cps)
    cps = rets.computation()
    assert('wic_ben' in cps)

    print('CPS Tax Units Created')
    filers, nonfilers = adjfilst(cps)

    print('Adjustment Complete')
    soi = create_soi(puf.copy())

    print('Start Phase One')
    filers = filers.fillna(0)
    soi = soi.fillna(0)
    soi_final, cps_final, counts = phaseone(filers, soi)

    print('Start Phase Two')
    match = phasetwo(
                soi_final.loc[:, ['cellid', 'soiseq', 'wt', 'factor', 'yhat']],
                cps_final.loc[:, ['cellid', 'cpsseq', 'wt', 'factor', 'yhat']])

    print('Creating final file')
    cpsrets = add_cps(filers, match, puf)
    assert('wic_ben' in cpsrets)
    cps_matched = add_nonfiler(cpsrets, nonfilers)
    assert('wic_ben' in cps_matched)
    # add age range variable
    cps_matched['agerange'] = 0
    # Rename variables for use in PUF data prep
    renames = {'icps1': 'age_head',
               'icps2': 'age_spouse',
               'wasp': 'wage_head',
               'wass': 'wage_spouse'}
    cps_matched = cps_matched.rename(columns=renames)

    return cps_matched


if __name__ == "__main__":
    cps_matched = match()
    print('Exporting Final File')
    cps_matched.to_csv('../../cps-matched-puf_ben.csv', index=False,
                       float_format='%.2f')
