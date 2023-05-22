# AUTOMATE DATA INGESTION PIPELINE PORTFOLIO PROJECT
### Build a data engineering pipeline to regularly transform a messy database into a clean resource for the analytics team

#### Info
- Mock database of long term cancelled subscribers for a fictional subscription company
	- DB is regularly updated from multiple sources, and needs to be routinely cleaned and transformed into usable shape with as little human intervention as possible
- Important to practice working with customer data, but this data is fictional as it would be unethical to share actual customer data
- Fictional Education Company Name: **Cademycode**

My Pipeline:
	- performs unit tests (is data valid?)
	- writes readable errors to an error log
	- checks and updates changelogs
	- updates production database with new (clean) data

#### Tasks
1. Use Jupyter Notebook to explore and clean dataset
2. Use Python to automate data cleaning and transformation using unit tests and error logging
3. Use Bash scripts to automate file management and run scripts

#### Instructions

1. Run `script.sh` and follow given prompts (1 for yes, 0 for no)
2. If prompted, will run `dev/cleanse_data.py` (runs unit tests and data cleaning on dev/cademycode.db)
3. Any errors encountered during testing by `cleanse_data.py` will raise exception, log, and terminate
4. If no errors, `cleanse_data.py` updates and cleans database and CSV with new entries/records
5. After update, number of new records and data is written to 'dev/changelog.md'
6. `script.sh` checks changelog for updates
7. If there are updates, `script.sh` requests your permission to overwrite the prod database
8. If you grant permission (1 for yes, 0 for no), `script.sh` will copy updated database to `/prod` folder

`dev/cleanse_data.py` runs on the dataset found in 'dev/cademycode.db' (can change the code on line 141)

#### Folder Structure
	- **script.sh** - bash script; runs `cleanse_data.py` and moves files to `/prod` (if prompted)
	##### dev
	- **changelog.md** - automatically updates each time `cleanse_data.py` runs; logs new records and tracks missing data
	- **cademycode.db** - database containing raw data of 3 tables:
		- `cademycode_students`: course data/demographic of cademycode students 
		- `cademycode_student_jobs`: student jobs 
		- `cademycode_courses`: table of career paths and hours of completion
	- **cademycode_updated.db** - updated version for testing update process
	- **cademycode_cleansed.db** - output from 'cleanse_data.py' that has 2 tables:
		- `cademycode_cleansed`: contains joined data from cleaned cademycode.db tables
		- `missing_data`: table containing incomplete data (left for analysts to look at)
	- **cleanse_data.py** - runs unit tests and cleans data from `cademycode.db`
	##### prod
	- **changelog.md** - copied from `/dev` when updated 
	- **cademycode_cleansed.db** - copied from `/dev` when updated
	- **cademycode_cleansed.csv** - CSV of clean table; overwrites on update
	##### notice
	- **writeup.md** - high-level write up; includes thought process and steps taken
	- **data_ingestion.ipynb** - jupyter notebook; initial data discovery and cleaning process