# This script 

import requests
import re
import pandas as pd

# URL to the raw version of:
# https://github.com/CUTR-at-USF/gtfs-realtime-validator/blob/8fd8cca45abeb06c7b7cb969d14342f753a4d280/gtfs-realtime-validator-lib/src/main/java/edu/usf/cutr/gtfsrtvalidator/lib/validation/ValidationRules.java
# file that we will parse to get the  validation rules
VALIDATION_RULES_JAVA = 'https://raw.githubusercontent.com/CUTR-at-USF/gtfs-realtime-validator/8fd8cca45abeb06c7b7cb969d14342f753a4d280/gtfs-realtime-validator-lib/src/main/java/edu/usf/cutr/gtfsrtvalidator/lib/validation/ValidationRules.java'

# version number?
VALIDATION_RULES_VERSION = '1.0.0'

# ordered list of the attributes defined for each rule in the Java code
RULE_ATTRIBUTES = ['rule_id', 'rule_severity', 'rule_title', 'rule_description', 'rule_occurrence_suffix']

# RULE_PATTERN matches the rule contents within the 'new ValidationRule(<rule contents>);' statements in the Java code
# each rule declaration contains 5 arguments: 'new ValidationRule("<rule ID>"", "<rule type>"", "<rule title>"", "<rule description>", "<rule suffix>");'
# the '".*?"' repeated item in the regex represents an individual argument within the rule declaration
# the '\s*,\s*' repeated item in the regex represents a separator between arguments -- a comma with optional whitespace around it
# (there are spaces and sometimes newlines next to the commas) 
RULE_RE_PATTERN = re.compile('new ValidationRule\((".*?"\s*,\s*".*?"\s*,\s*".*?"\s*,\s*".*?"\s*,\s*".*?"\s*)\);', re.DOTALL)

# ATTRIBUTE_PATTERN pulls out the individual arguments within the rules identified by RULE_PATTERN
ATTRIBUTE_RE_PATTERN = re.compile('"(.*?)"', re.DOTALL)


def get_rules(VALIDATION_RULES_JAVA):
    # Pull 
    r = requests.get(VALIDATION_RULES_JAVA)
    try:
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(e)
        return ''

def parse_java_rules(raw_java, rule_pattern, attribute_pattern, attribute_list):
    '''Parse the raw Java code into a dataframe.
    Args:
        raw_java: Raw Java code (string)
        rule_pattern: Compiled regex pattern that matches rule declarations within the raw Java
        attribute_pattern: Compiled regex pattern that matches individual attributes within a rule declaration in the raw Java
        attribute_list: Ordered list of the names of the rule attributes defined in each rule declaration
    '''

    rules_df = pd.DataFrame(columns = attribute_list)

    for rule in rule_pattern.findall(raw_java):
        attributes = tuple(attribute_pattern.findall(rule))
        try:
            assert len(attributes) == 5
            attribute_row = pd.DataFrame(attributes, index = attribute_list).transpose()
            rules_df = rules_df.append(attribute_row)
        except AssertionError as e:
            print("Failed to parse rules. Rule {} had unexpected number of attributes (should be 5).".format(rule))

    return rules_df

def create_table(rules_df):
    '''If views.gtfs_rt_dim_validation_rules doesn't exist, then create it.'''
    from calitp import write_table

    write_table(
        rules_df,
        f'views.gtfs_rt_dim_validation_rules',
        replace=False,
        if_not_exists=True
    )

raw_java = get_rules(VALIDATION_RULES_JAVA)
rules = parse_java_rules(raw_java, RULE_RE_PATTERN, ATTRIBUTE_RE_PATTERN, RULE_ATTRIBUTES)

print(rules)