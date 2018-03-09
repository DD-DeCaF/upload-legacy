# Copyright 2018 Novo Nordisk Foundation Center for Biosustainability, DTU.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
synonym_to_chebi_name_dict = {
    'o2': 'dioxygen',
    'co2': 'carbon dioxide',
    'tryptophan': 'L-tryptophan',
    'trp': 'L-tryptophan',
    'gluc': 'aldehydo-D-glucose',
    'glucose': 'aldehydo-D-glucose',
    'glc': 'aldehydo-D-glucose',
    'methionine': 'L-methionine',
    'tyrosine': 'L-tyrosine',
    'tyr': 'L-tyrosine',
    'yrosine': 'L-tyrosine',
    'chloramphenicol': 'chloramphenicol',
    'chlorampenicol': 'chloramphenicol',
    '5htp': '5-hydroxytryptophan',
    '5-htp': '5-hydroxytryptophan',
    'spectinomycin': 'spectinomycin',
    'spectinomycine': 'spectinomycin',
    'kanamycin': 'kanamycin X',
    'indole': '1H-indole',
    'oxygen': 'dioxygen',
    'succinate': 'succinate(1-)',
    'lactate2': 'lactate',
    'trp_ex': 'L-tryptophan',
    'trp_total': 'L-tryptophan',
    'trpytamine': 'tryptamine',
    'htp': '5-hydroxytryptophan',
    'acetylserotonin': 'N-acetylserotonin',
    'acetyltryptamine': 'N-acetyltryptamine',
    'acserotonin': 'N-acetylserotonin',
    'actyptamine': 'N-acetyltryptamine',
    '(nh4)2so4': 'ammonium sulfate',
    'cacl2*2h2o': 'calcium dichloride',
    'cocl2*6h2o': 'cobalt dichloride',
    'edta': 'EDTA disodium salt dihydrate',
    'h3bo3': 'boric acid',
    'kh2po4': 'potassium dihydrogen phosphate',
    'ki': 'potassium iodide',
    'mgcl2': 'magnesium dichloride',
    'mgso4*7h2o': 'magnesium sulfate heptahydrate',
    'na2moo4*2h2o': 'sodium molybdate dihydrate',
    'p-aminobenzoic acid': '4-aminobenzoic acid',
    'znso4*7h2o': 'zinc sulfate heptahydrate',
    'CoSO4*6H2O': 'cobalt(2+) sulfate heptahydrate',
    'Na2MoO2*2H2O': 'sodium molybdate dihydrate',
    'NiSO4*3H2O': 'nickel sulfate',
    'EtOH': 'ethanol',
    'Biotin': 'biotin',
    '3HP': '3-hydroxypropionic acid',
    'MES': '2-(N-morpholino)ethanesulfonic acid',
    'mncl2*4h2o': 'manganese(II) chloride tetrahydrate',
    'cuso4*5h2o': 'copper(II) sulfate pentahydrate',
    'ZnCl2': 'zinc dichloride',
    'FeSO4*7H2O': 'iron(2+) sulfate heptahydrate',
    'tri-Na-citrate': 'sodium citrate',
    'Al2(SO4)3*18 H2O': 'aluminium sulfate octadecahydrate',
    'Al2(SO4)3*18H2O': 'aluminium sulfate octadecahydrate',
    'MnSO4*H2O': 'manganese(II) sulfate monohydrate',
    'Pyridoxine HCl': 'pyridoxine hydrochloride',
    'Pyridoxine-HCl': 'pyridoxine hydrochloride',
    'Thiamine HCl': 'thiamine(2+) dichloride',
    'Thiamine-HCl': 'thiamine(2+) dichloride',
    'Ca-D-(+)phantothenate': 'Calcium pantothenate',
    'Ca-panthothenate': 'pantothenate',
    'Thiotic acid': '(R)-lipoic acid',
    'Vitamin B12': 'cobalamin',
    'Glucose*H2O': 'aldehydo-D-glucose',
    'Tryptophane': 'L-tryptophan',
    'NAcetyltryptamine': 'N-acetyltryptamine',
    'NAcTrp': 'N-acetyltryptamine',
    'Yeast Extract': 'Yeast extract'
}

skip_list = {'Antifoam 204'}


def measurement_test(unit, parameter, numerator_compound, denominator_compound, quantity):
    unit_dict = {'mg/L': {'numerator': {'quantity': 'mass', 'unit': 'mg'},
                          'denominator': {'quantity': 'volume', 'unit': 'L'}},
                 'Cmol/Cmol': {'numerator': {'quantity': 'amount', 'unit': 'Cmol'},
                               'denominator': {'quantity': 'amount', 'unit': 'Cmol'}},
                 'g/L': {'numerator': {'quantity': 'mass', 'unit': 'g'},
                         'denominator': {'quantity': 'volume', 'unit': 'L'}},
                 'g CDW/L': {'numerator': {'quantity': 'mass', 'unit': 'g'},
                             'denominator': {'quantity': 'volume', 'unit': 'L'}},
                 'h-1': {'rate': 'h'},
                 'nan': {'numerator': {'quantity': 'carbon-balance'}},
                 'g CDW/mol': {'numerator': {'quantity': 'CDW', 'unit': 'g'},
                               'denominator': {'quantity': 'amount', 'unit': 'mol'}},
                 'mmol/gCDW': {'numerator': {'quantity': 'amount', 'unit': 'mmol'},
                               'denominator': {'quantity': 'CDW', 'unit': 'g'}},
                 'mg/gCDW': {'numerator': {'quantity': 'mass', 'unit': 'mg'},
                             'denominator': {'quantity': 'CDW', 'unit': 'g'}},
                 'mmol/(gCDW*h)': {'numerator': {'quantity': 'amount', 'unit': 'mmol'},
                                   'denominator': {'quantity': 'CDW', 'unit': 'g'}, 'rate': 'h'},
                 'mg/(gCDW*h)': {'numerator': {'quantity': 'mass', 'unit': 'mg'},
                                 'denominator': {'quantity': 'CDW', 'unit': 'g'}, 'rate': 'h'}}
    test_description = unit_dict[str(unit)]
    test_description['type'] = parameter
    if str(numerator_compound) != 'nan':
        test_description['numerator']['compounds'] = [numerator_compound]
    if str(denominator_compound) != 'nan':
        test_description['denominator']['compounds'] = [denominator_compound]
    if str(quantity) != 'nan':
        test_description['numerator']['quantity'] = quantity
    return test_description

compound_skip = 'compound-on-skip-list'