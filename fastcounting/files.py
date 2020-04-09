"""
The place were we read all reports.
Like csv, txt, or excel files and return pandas Dataframes.
"""
import datetime as dt
import pandas as pd

from fastcounting import helper
# right now for each report we only reed one random file
# entry points are: main_etl(month) and main_summe(month)

# our main ETL to fill our database with
def find_batch_files(month):
    p = helper.Helper().datafolder(month)
    files = [file for file in p.iterdir() if file.parts[-1].lower().startswith('journal')]
    return files

def read_lexware_journal(files, nrows=None):
    """Read xlxs from the default folder for each year e.g. month=2018-13 or actual month."""
    data = pd.read_excel(
        files[0], bom=True, sep=';', encoding='latin-1', decimal=',', thousands='.',
        dayfirst=True, skiprows=1, parse_dates=['Belegdat.', 'Buchdat.', 'Jour. Dat.'], nrows=nrows)
    return data

def clean_lexware_journal(df):
    for column in ['Belegdat.', 'Buchdat.', 'Jour. Dat.']:
        df[column] = df[column].ffill() # this works cause data is sorted
        df[column] = (df[column] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
        df[column] = df[column].apply(str)

    # multiply all currency amounts to get integers
    for money_column in ['SollEUR', 'HabenEUR', 'USt H-EUR', 'USt-S EUR']:
        df[money_column] = df[money_column]*100
        # we don't cast to integer because we have slightly meaningful nans
        df[money_column] = df[money_column].round(0)

    dimensions = ['Sollkto', 'Habenkto', 'USt Kto-H', 'USt Kto-S']
    df.fillna(value={dimension: 0.0 for dimension in dimensions}, inplace=True)
    return df

def main_etl(month):
    """All methods should run together here, pandas read is slow."""
    files = find_batch_files(month)
    df = read_lexware_journal(files)
    df = clean_lexware_journal(df)
    return [df, files[0].parts[-1]]


# txt file export from Lexware "Summe und Salden"
def read_summe(month, name='report', nrows=None):
    """Read xlxs from the default folder for each year e.g. month=2018-13 or actual month."""
    p = helper.Helper().datafolder(month)
    files = [file for file in p.iterdir() if file.parts[-1].lower().startswith(name)]
    if files:
        data = pd.read_table(files[0], sep='\t', engine='python', header=[0, 1], decimal=',', thousands='.', parse_dates=[2])
    return data

def clean_summe(validate):
    better_columns = ['Konto', 'Name', 'Letzte Buchung', 'EB Soll', 'EB Haben', 'Summe Soll', 'Summe Haben', 'drop1', 'drop2', 'Saldo Soll', 'Saldo Haben']
    validate.columns = better_columns
    validate.set_index('Konto', inplace = True)
    validate.fillna(0, inplace=True)  # for full year data set they are equal to sum
    validate.drop(['drop1', 'drop2'], axis=1, inplace=True)
    return validate


def main_summe(month):
    """Month has format of 2020-12 and for year 2020-13."""
    return clean_summe(read_summe('2017-13'))