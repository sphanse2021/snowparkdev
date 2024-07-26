from snowflake.core import Root 
import snowflake.connector
from datetime import timedelta
from snowflake.core.task import Task , StoredProcedureCall
import procedures
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
from snowflake.core.task.context import TaskContext

from snowflake.core.task.dagv1 import DAG , DAGTask , DAGOperation , CreateMode , DAGTaskBranch


#conn = snowflake.connector.connect()

print("****** snowflake account ******")
print(f'$SNOWFLAKE_ACCOUNT')

conn = snowflake.connector.connect(
    user=f'$SNOWFLAKE_USER',
    password=f'$SNOWFLAKE_PASSWORD',
    account=f'$SNOWFLAKE_ACCOUNT',
    warehouse=f'$SNOWFLAKE_WAREHOUSE',
    database=f'$SNOWFLAKE_DATABASE')

print("connection established")
print(conn)

root = Root(conn)
print(root)

# crete definiton for the task 

my_task = Task("my_task",StoredProcedureCall(procedures.hello_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh",schedule=timedelta(hours=1))

tasks = root.databases["demo_db"].schemas['public'].tasks
#tasks.create(my_task)

# create dag  
with DAG("my_dag",schedule=timedelta(days=1),use_func_return_value=True,stage_location="@dev_deployment") as dag:
  dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,args=["pradeep"],\
    input_types=[StringType()],return_type=StringType(), packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_2 =  DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_3 =  DAGTask("my_test_task3",StoredProcedureCall(procedures.test_procedure_two,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_4 =  DAGTask("my_test_task4",StoredProcedureCall(procedures.test_procedure,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_1 >> dag_task_2 >> [dag_task_3,dag_task_4]
  
  schema = root.databases["demo_db"].schemas["public"]
  dag_op = DAGOperation(schema)
  dag_op.deploy(dag,CreateMode.or_replace)


# execute dag using task branch

def task_brach_condition(session: Session) -> str:
  # write conditon
  return "my_test_task3"

with DAG("my_dag_task_branch",schedule=timedelta(days=1),stage_location="@dev_deployment",packages=["snowflake-snowpark-python"]) as dag:
  dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,args=["pradeep"],\
  input_types=[StringType()],return_type=StringType(), packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  # dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(procedures.test_procedure,\
  #   packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
  #   stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_2 =  DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_3 =  DAGTask("my_test_task3",StoredProcedureCall(procedures.test_procedure_two,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_4 =  DAGTask("my_test_task4",StoredProcedureCall(procedures.test_procedure,\
    packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_branch = DAGTaskBranch("task_branch",task_brach_condition,warehouse="compute_wh")
  
  dag_task_1 >> dag_task_2 >> dag_task_branch
  
  dag_task_branch >> [dag_task_3,dag_task_4]
  
  schema = root.databases["demo_db"].schemas["public"]
  dag_op = DAGOperation(schema)
  dag_op.deploy(dag,CreateMode.or_replace)
  
  
#   # Return value from the task.
  
def send_task_value(session:Session) -> None:
  from snowflake.core.task.context import TaskContext
  # processing by writing code.
  context = TaskContext(Session)
  context.set_return_value("value passed by task-2")
    
def receive_task_value(session:Session) -> None:
  # processing by writing code.
  context = TaskContext(Session)
  return_value = context.get_predecessor_return_value("my_hello_task")
  #return return_value
    
  
with DAG("my_dag_send_task_value",schedule=timedelta(days=1),stage_location="@dev_deployment",packages=["snowflake-snowpark-python"]) as dag:
  
  dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(send_task_value,\
  packages=["snowflake-snowpark-python","snowflake"],\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
  # dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(procedures.test_procedure,\
  #   packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
  #   stage_location="@dev_deployment"),warehouse="compute_wh")
  
  dag_task_2 =  DAGTask("my_test_task",StoredProcedureCall(receive_task_value,\
  packages=["snowflake-snowpark-python"],\
  stage_location="@dev_deployment"),warehouse="compute_wh")
    
  dag_task_1 >> dag_task_2
  
  schema = root.databases["demo_db"].schemas["public"]
  dag_op = DAGOperation(schema)
  dag_op.deploy(dag,CreateMode.or_replace)
  
  






















# execute dag task based on condition.

# def task_branch_condition(session: Session) -> str:
#   # Data processing code
#   return "my_test_task3"

# with DAG("my_dag_branch3",schedule=timedelta(days=1),use_func_return_value=True,stage_location="@dev_deployment") as dag:
  
#   dag_task_1 =  DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,args=["pradeep"],\
#     packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
#     stage_location="@dev_deployment"),warehouse="compute_wh")
  
#   dag_task_2 =  DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
#     packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
#     stage_location="@dev_deployment"),warehouse="compute_wh")
  
#   dag_task_3 =  DAGTask("my_test_task3",StoredProcedureCall(procedures.test_procedure,\
#     packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
#     stage_location="@dev_deployment"),warehouse="compute_wh")
  
#   dag_task_4 =  DAGTask("my_test_task4",StoredProcedureCall(procedures.test_procedure,\
#     packages=["snowflake-snowpark-python"],imports=["@dev_deployment/my_snowpark_project/app.zip"],\
#     stage_location="@dev_deployment"),warehouse="compute_wh")
  
#   dag_task_branch = DAGTaskBranch("task_branch",task_branch_condition,warehouse="compute_wh")
  
#   dag_task_1 >> dag_task_2 >> dag_task_branch
#   dag_task_branch >> [dag_task_3,dag_task_4]
  
#   schema = root.databases["demo_db"].schemas["public"]
#   dag_op = DAGOperation(schema)
#   dag_op.deploy(dag,CreateMode.or_replace)
    
    





