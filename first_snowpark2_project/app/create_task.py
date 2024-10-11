from snowflake.core import Root
import snowflake.connector 
from datetime import timedelta
from snowflake.core.task import Task, StoredProcedureCall
# app import procedures 
import procedures
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, CreateMode, DAGTaskBranch
from snowflake.snowpark import Session

conn = snowflake.connector.connect()
print("connection established")
root = Root(conn)

#create task
my_task = Task("my_task", StoredProcedureCall(procedures.hello_procedure,stage_location="@dev_deployment" ),warehouse="compute_wh", schedule=timedelta(days=1) )
tasks = root.databases["demo_db"].schemas["public"].tasks
tasks.create(my_task)  


def task_branch_condition(session:Session) -> str:
  # Some business logic
  return "my_test_task3"
    
# create dag task
with DAG("my_dag", schedule=timedelta(days=1),warehouse="compute_wh") as dag:
    dag_task_1 = DAGTask("my_hello_task", StoredProcedureCall(procedures.hello_procedure, args=["Sudhir"],\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")
    
    dag_task_2 = DAGTask("my_test_task", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

    dag_task_3 = DAGTask("my_test_task3", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

    dag_task_4 = DAGTask("my_test_task4", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

dag_task_1 >> dag_task_2 >> [dag_task_3, dag_task_4]

schema = root.databases["demo_db"].schemas["public"]
dag_op = DAGOperation(schema)  
dag_op.deploy(dag, CreateMode.or_replace)
 
   
# create dag branch task
with DAG("my_dag_task_branch", schedule=timedelta(days=1),packages=["snowflake-snowpark-python"],use_func_return_value=True,\
         warehouse="compute_wh") as dag:
    #dag_task_1 = DAGTask("my_hello_task", StoredProcedureCall(procedures.hello_procedure, args=["Sudhir"],\
    #  packages=["snowflake-snowpark-python"],
    #  imports=["@dev_deployment/first_snowpark2_project/app.zip"],
    #  stage_location="@dev_deployment" ),warehouse="compute_wh")
    
    dag_task_1 = DAGTask("my_test_task1", StoredProcedureCall(procedures.test_procedure, \
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")
    
    dag_task_2 = DAGTask("my_test_task2", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

    dag_task_3b = DAGTask("my_test_task3", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

    dag_task_4b = DAGTask("my_test_task4", StoredProcedureCall(procedures.test_procedure,\
      packages=["snowflake-snowpark-python"],
      imports=["@dev_deployment/first_snowpark2_project/app.zip"],
      stage_location="@dev_deployment" ),warehouse="compute_wh")

    dag_task_branch = DAGTaskBranch("task_branch", task_branch_condition, warehouse="compute_wh")
    
    dag_task_1 >> dag_task_2 >> [dag_task_3b, dag_task_4b]

schema = root.databases["demo_db"].schemas["public"]
dag_op2 = DAGOperation(schema)  
dag_op.deploy(dag, CreateMode.or_replace)