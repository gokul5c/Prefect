from main import read_root
from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=read_root,
    name="click_connect",
    schedule=(CronSchedule(cron="*/5 * * * *", timezone="Asia/Kolkata")),
    version=1, 
    work_queue_name="click_work",
    tags = ['transactions']    
)

if __name__ == '__main__':
    print('connected')
    deployment.apply()