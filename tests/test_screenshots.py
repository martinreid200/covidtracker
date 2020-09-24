from selenium import webdriver
import os
import time
import sys
import imageio
from scipy.linalg import norm
from numpy import sum, average
import re
import pytest

testdir = os.path.dirname(__file__)
srcdir = '../app/src'
sys.path.insert(0, os.path.abspath(os.path.join(testdir, srcdir)))

from app import header

STAGING_URL = 'http://192.168.1.170:5200'
PRODUCTION_URL = 'http://mrvm1.uksouth.cloudapp.azure.com'
driver = None

# Selenium options
options = webdriver.FirefoxOptions()
options.add_argument('-headless')
driver = webdriver.Firefox(options=options)
driver.set_window_size(1920, 1080)


def capture_screens(link):
    screenshot(STAGING_URL+"/"+link, 'screen_staging.png')
    screenshot(PRODUCTION_URL+"/"+link, 'screen_production.png')

def image_path(file_name):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'screenshots', file_name)

def screenshot(url, file_name):
    print ("Capturing", url, "screenshot as", file_name, "...")
    driver.get(url)
    time.sleep(5)
    driver.save_screenshot(image_path(file_name))
    driver.get_screenshot_as_png()
    #htopprint ("Done.")

def compare():
    
    file1 = image_path('screen_staging.png') 
    file2 = image_path('screen_production.png') 

    img1 = imageio.imread(file1).astype(float)
    img2 = imageio.imread(file2).astype(float)

    n_m, n_0 = compare_images(img1, img2)
    print ("Manhattan norm:", n_m)
    if n_m == 0:
        return "same"
    else:
        return "different"


def compare_images(img1, img2):

    # calculate the difference and its norms
    diff = img1 - img2  # elementwise for scipy arrays
    m_norm = sum(abs(diff))  # Manhattan norm
    z_norm = norm(diff.ravel(), 0)  # Zero norm
    return (m_norm, z_norm)


# get all links in app layout header
items = str(header).split(",")
rawlinks = [x for x in items if x.startswith(" href='/link")]
links = ["/"]
for rawlink in rawlinks:
    href = re.findall(r"'(.*?)'", rawlink)
    links.append(href[0])

print(links)

@pytest.mark.parametrize('link', links)
def test_screenshots(link):
    
    print("\nTesting link:",link)
    capture_screens(link)
    assert compare() == "same"

    # clean up if we're at last link
    if link == links[-1]:
        driver.close()


#if __name__ == '__main__':
#    test_screenshots()
    