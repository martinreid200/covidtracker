import datetime
import os, sys

testdir = os.path.dirname(__file__)
srcdir = '../app/src'
sys.path.insert(0, os.path.abspath(os.path.join(testdir, srcdir)))

from dataframes import CasesData

cases = CasesData()

def test_latest_cases():

    dt = datetime.datetime.strptime(cases.latest_case_date, '%d/%m/%Y')
    print("Latest Cases",dt)
    td = datetime.datetime.strptime((datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y'), '%d/%m/%Y')
    print("Should have data up to at least",td)

    assert dt >= td#.strftime('%d/%m/%Y')
#    assert cases.latest_case_date >= datetime.datetime.today().strftime('%d/%m/%Y')

def test_dailydf():

    assert len(cases.dailydf) > 80000

def test_weeklydf():
    
    assert len(cases.weeklydf) > 11000

def test_summarydf():

    assert len(cases.summarydf) > 470


