#!/usr/bin/env python

"""
Tests for Stage 017's function get_category().
Usage: 'python -m unittest discover' from ..(directory with Stage 017 code).
"""

import unittest
import adjustMetadata


class Case(unittest.TestCase):
    def setUp(self):
        self.task = {'hashtag_list': [], 'taskname': ''}

    def tearDown(self):
        self.task = None

    def test_empty(self):
        self.assertEqual(adjustMetadata.get_category(self.task),
                         ['Uncategorized'])

    def test_multiple_tags(self):
        self.task['hashtag_list'] = ['btagging', 'diphoton', 'qcd']
        result_function = set(adjustMetadata.get_category(self.task))
        result_known = {'BTag', 'GammaJets', 'Multijet'}
        self.assertEqual(result_function, result_known)

    def test_multiple_tags_same_category(self):
        self.task['hashtag_list'] = ['diboson', 'zz', 'ww']
        self.assertEqual(adjustMetadata.get_category(self.task), ['Diboson'])

    def test_multiple_phys_shorts(self):
        self.task['taskname'] = 'nothing.nothing.3topjetstanb_wenu_'
        result_function = set(adjustMetadata.get_category(self.task))
        result_known = {'TTbarX', 'Multijet', 'SUSY', 'Wjets'}
        self.assertEqual(result_function, result_known)

    def test_phys_short_wrong_field(self):
        self.task['taskname'] = '3top.jets.nothing.tanb._wenu_'
        self.assertEqual(adjustMetadata.get_category(self.task),
                         ['Uncategorized'])


PHYS_CATEGORIES_MAP = {
    'BPhysics': ['charmonium', 'jpsi', 'bs', 'bd', 'bminus', 'bplus',
                 'charm', 'bottom', 'bottomonium', 'b0'],
    'BTag': ['btagging'],
    'Diboson': ['diboson', 'zz', 'ww', 'wz', 'wwbb', 'wwll'],
    'DrellYan': ['drellyan', 'dy'],
    'Exotic': ['exotic', 'monojet', 'blackhole', 'technicolor',
               'randallsundrum', 'wprime', 'zprime', 'magneticmonopole',
               'extradimensions', 'warpeded', 'randallsundrum',
               'contactinteraction', 'seesaw'],
    'GammaJets': ['photon', 'diphoton'],
    'Higgs': ['whiggs', 'zhiggs', 'mh125', 'higgs', 'vbf', 'smhiggs',
              'bsmhiggs', 'chargedhiggs'],
    'Minbias': ['minbias'],
    'Multijet': ['dijet', 'multijet', 'qcd'],
    'Performance': ['performance'],
    'SingleParticle': ['singleparticle'],
    'SingleTop': ['singletop'],
    'SUSY': ['bino', 'susy', 'pmssm', 'leptosusy', 'rpv', 'mssm'],
    'Triboson': ['triplegaugecoupling', 'triboson', 'zzw', 'www'],
    'TTbar': ['ttbar'],
    'TTbarX': ['ttw', 'ttz', 'ttv', 'ttvv', '4top', 'ttww'],
    'Upgrade': ['upgrad'],
    'Wjets': ['w'],
    'Zjets': ['z']
}


PHYS_CATEGORIES_PHYS_SHORTS_MAP = {
    'SingleTop': ['singletop', '_wt', '_wwbb'],
    'TTbar': ['ttbar', '_tt_'],
    'Multijet': ['jets'],
    'Higgs': ['h125', 'xhh'],
    'TTbarX': ['ttbb', 'ttgamma', '3top'],
    'BPhysics': ['upsilon'],
    'SUSY': ['tanb'],
    'Exotic': ['4topci'],
    'Wjets': ['_wenu_']
}


'''
Functions for adding tests in a cycle. They are required to avoid f() being
stuck on the last value in the loop.
'''


def add_tag_test(category, tag):
    def f(self):
        self.task['hashtag_list'] = [tag]
        self.assertEqual(adjustMetadata.get_category(self.task), [category])
    setattr(Case, 'test_category_%s_tag_%s' % (category, tag), f)


def add_phys_short_test(category, phys_short):
    def f(self):
        self.task['taskname'] = 'nothing.nothing.' + phys_short
        self.assertEqual(adjustMetadata.get_category(self.task), [category])
    setattr(Case, 'test_category_%s_phys_short_%s'
                  % (category, phys_short), f)


# Add tests by cycling over values in dictionary.
for category in PHYS_CATEGORIES_MAP:
    for tag in PHYS_CATEGORIES_MAP[category]:
        add_tag_test(category, tag)


for category in PHYS_CATEGORIES_PHYS_SHORTS_MAP:
    for phys_short in PHYS_CATEGORIES_PHYS_SHORTS_MAP[category]:
        add_phys_short_test(category, phys_short)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(Case))
    return suite
