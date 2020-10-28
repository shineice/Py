from selenium import webdriver
from selenium.webdriver.support.ui import Select

driver =webdriver.Chrome()
driver.get("https://my.ntu.edu.tw/vision/manufacturers/ntuvision.html")
time.sleep(0.5)
driver.find_element_by_id("user").clear()
driver.find_element_by_id("user").send_keys("84598349")
driver.find_element_by_id("password").clear()
driver.find_element_by_id("password").send_keys("104super")
driver.find_element_by_xpath('//*[@id="BTdelete"]').click()
driver.get("https://my.ntu.edu.tw/vision/manufacturers/fair.html")
for i in range(3):
    driver.find_element_by_id("CBclassType").clear()
    select.select_by_index(0)
    driver.find_element_by_id("titlename").clear()
    driver.find_element_by_id("titlename").send_keys("104")
    driver.find_element_by_id("standnumber").clear()
    driver.find_element_by_id("standnumber").send_keys("1")
    driver.find_element_by_id("Submit1").click()
    driver.refresh()