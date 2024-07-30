# Processing Zooniverse Data Exports
# Use Case: Mutli-task workflow -- text

# Define the filenames
filename_classifications = 'butterfly-specimens-nmmnh-and-s-classifications July 30 2024.csv'
filename_output = 'butterfly-specimens-classifications_July 30 2024_flat+trim.csv'

# Define the columns you want to select
columns_out = ['classification_id', 'created_at', 'user_name', 'user_id',
               'workflow_id', 'workflow_version', 'subject_ids', 
               'taskvalue_species', 'taskvalue_sex', 'taskvalue_locality',
               'taskvalue_elevation', 'taskvalue_collector_number', 'taskvalue_date', 
               'taskvalue_pinning_view', 'taskvalue_county', 'taskvalue_state', 'taskvalue_misc', 'subject_data']

# Reference: column names to choose from
columns_in = ['classification_id', 'user_name', 'user_id', 'user_ip', 
              'workflow_id','workflow_name', 'workflow_version', 'created_at', 
              'gold_standard', 'expert', 'metadata', 'annotations', 
              'subject_data', 'subject_ids']
       
columns_new = ['metadata_json', 'annotations_json', 'subject_data_json', 
               'taskvalue_text', 'taskvalue_survey']

import pandas as pd
import json

# Read the input CSV file into a DataFrame
classifications = pd.read_csv(filename_classifications)

# Convert JSON columns to dictionaries
classifications['metadata'] = [json.loads(q) for q in classifications.metadata]
classifications['annotations'] = [json.loads(q) for q in classifications.annotations]
classifications['subject_data'] = [json.loads(q) for q in classifications.subject_data]

#Flatten Annotations

# Initialize new fields in DataFrame
classifications['taskvalue_species'] = ''
classifications['taskvalue_sex'] = ''
classifications['taskvalue_locality'] = ''
classifications['taskvalue_elevation'] = ''
classifications['taskvalue_collector_number'] = ''
classifications['taskvalue_date'] = ''
classifications['taskvalue_pinning_view'] = ''
classifications['taskvalue_county'] = ''
classifications['taskvalue_state'] = ''
classifications['taskvalue_misc'] = ''

for i,row in classifications.iterrows():

  for t in row['annotations']:
    # Species Task = T0
    if t['task'] == 'T0':
      classifications.loc[i,'taskvalue_species'] = t['value'].rstrip()
    
    # Sex Task = T1
    if t['task'] == 'T1':
      classifications.loc[i,'taskvalue_sex'] = t['value'].rstrip()

    # Locality Task = T2
    if t['task'] == 'T2':
      classifications.loc[i,'taskvalue_locality'] = t['value'].rstrip()

    # Elevation Task = T4
    if t['task'] == 'T4':
      classifications.loc[i,'taskvalue_elevation'] = t['value'].rstrip()

    # Collector Number Task = T5
    if t['task'] == 'T5':
      classifications.loc[i,'taskvalue_collector_number'] = t['value'].rstrip()

    # Date Task = T6
    if t['task'] == 'T6':
      classifications.loc[i,'taskvalue_date'] = t['value'].rstrip()
    
    # View Task = T7
    if t['task'] == 'T7':
      classifications.loc[i,'taskvalue_pinning_view'] = t['value'].rstrip()

    # County Task = T8
    if t['task'] == 'T8':
      classifications.loc[i,'taskvalue_county'] = t['value'].rstrip()

    # State Task = T9
    if t['task'] == 'T9':
      classifications.loc[i,'taskvalue_state'] = t['value'].rstrip()

    # Misc Task = T10
    if t['task'] == 'T10':
      classifications.loc[i,'taskvalue_misc'] = t['value'].rstrip()


# Select and save the columns to the CSV
output = classifications[columns_out]
output.to_csv(filename_output, index=False)

print(f'Selected columns have been saved to {filename_output}')
