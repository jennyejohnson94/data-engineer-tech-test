# data-engineer-tech-test

### Task 1
Write an Apache Beam batch job in Python satisfying the following requirements 
1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
2. Find all transactions have a `transaction_amount` greater than `20`
3. Exclude all transactions made before the year `2010`
4. Sum the total by `date`
5. Save the output into `output/results.jsonl.gz`

### Task 2
Following up on the same Apache Beam batch job, also do the following 
1. Group all transform steps into a single `Composite Transform`
1. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam

---

#### Requirements
- python3
- `pip install apache-beam`

#### Running the tasks
Note: you will have to delete the result file to be able to run a task again
- task 1: `python task_1.py`
- task 2: `python task_2.py`

#### Running the tests
`python -m unittest tests.test_task_2.TestTotalAmountPerDate`