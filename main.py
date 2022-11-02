import schedule
import time
from common import EnergyPlusDeliverFilter
def job():
    common.EnergyPlusDeliverFilter()
schedule.every().hour.do(job)
while True:
    schedule.run_pending()