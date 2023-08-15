import time

from vantage6.client import Client

print("Attempt login to Vantage6 API")
client = Client("http://localhost", 5000, "/api")
client.authenticate("user", "password")

client.setup_encryption(None)

categorical_variables = ["sex", "education_category", "education_category_3", "smoker", "alcohol", 
                         "hypertension", "drugs_hypertension", "drugs_hypercholesterolemia", "depression", 
                         "dm_1", "dm_2", "cardiovascular_disease"]

continuous_variables = ["birth_year", "age", "physical_activity", "bmi", "waist", "sbp", "dbp",
                        "hdl_ratio", "mmse_total"]

input_ = {
    "master": True,
    "method":"master",
    "args": [], 
    #"input_format": "json",
    "output_format": "json", 
    "kwargs": {
        #"functions": ["min", "max"],
        "columns":
            # [
            #     {
            #         "variable": variable,
            #         "table": "ncdc",
            #         "functions": ["count_null", "count", "count_discrete"],
            #     } 
            #     for variable in categorical_variables
            # ],
            [
                {
                    "variable": variable,
                    "table": "ncdc",
                    "functions": ["min", "max", "avg", "pooled_std", "count_null"],
                } 
                for variable in continuous_variables
            ],
        # "cohort": {
        #     "definition": [
        #         {
        #             "variable": "birth_year",
        #             "operator": ">=",
        #             "value": 1940,
        #         },
        #         {
        #             "variable": "education_years",
        #             "operator": ">=",
        #             "value": "'3'",
        #         },
        #     ],
        #     "table": "ncdc",
        #     "id_column": "id"
        # },
        "org_ids": [4]
    }
}

print("Requesting to execute summary algorithm")
task = client.post_task(
    name="testing",
    image="pmateus/v6-summary-rdb:1.3.0",
    collaboration_id=1,
    input_= input_,
    organization_ids=[4]
)

task_id = task.get("id")
print(f"task id={task_id}")

task = client.request(f"task/{task_id}")
while not task.get("complete"):
    task = client.request(f"task/{task_id}")
    print("Waiting for results...")
    time.sleep(1)

# 5. obtain the finished results
results = client.get_results(task_id=task.get("id"))

# e.g. print the results per node
# print(results)
print((results[0]['result']))
