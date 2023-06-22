import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag=DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

end_point="http://ll.thespacedevs.com/2.0.0/launch/upcoming"
download_path="/Users/black/dev/bigdata/workflow/data"
bash_command="curl -o {0} -L '{1}'".format(end_point+"/launches.json", end_point)

download_launches=BashOperator(
    # bash_command=bash_command,
    task_id="download_launches",
    bash_command="curl -o /Users/black/dev/bigdata/workflow/data/launches.json -L 'http://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    with open(download_path+"/launches.json") as f:
        launches=json.load(f)
        image_urls=[launch["image"] for launch in launches["results"]]
        print(f"image_urls => {image_urls}")
        for image_url in image_urls:
            try:
                print(f"image_url => {image_url}")
                response=requests.get(image_url)
                print(f"response => {response.ok}")
                image_filename=image_url.split("/")[-1]
                print(f"image_filename => {image_filename}")
                #target_file=f"{download_path}/images/{image_filename}"
                target_file=f"/tmp/images/{image_filename}"
                print(f"target_file => {target_file}")
                if image_filename == "spaceshiptwo_image_20210522140909.jpeg":
                    with open(target_file, "wb") as fa:
                        fa.write(response.content)
                    print(f"Download {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")
            except:
                print(f"Error ...")
            finally:
                print("finally")

get_pictures=PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify=BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls {0} | wc -l) images."'.format(download_path+"/images"),
    dag=dag
)

# if __name__ == '__main__':
#     _get_pictures()

download_launches >> get_pictures >> notify