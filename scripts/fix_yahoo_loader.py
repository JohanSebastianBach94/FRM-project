import re

# Read file
with open('c:/Users/frank/Documents/FRM project/data_pipeline/stress_indicators.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the problematic line
old_line = "df = pd.read_csv(file_path, index_col=0, parse_dates=[0])"
new_line = "# Read with skiprows to skip Ticker and date header rows\n        df = pd.read_csv(file_path, skiprows=[1, 2], index_col=0, parse_dates=True)"

content_new = content.replace(old_line, new_line)

# Write back
with open('c:/Users/frank/Documents/FRM project/data_pipeline/stress_indicators.py', 'w', encoding='utf-8') as f:
    f.write(content_new)

print('✓ Fixed Yahoo data loader - added skiprows=[1, 2]')
