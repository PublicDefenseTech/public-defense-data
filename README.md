# Tyler Technologies Odyssey scraper and parser

This is a scraper to collect and process public case records from the Tyler Technologies Odyssey court records system. If you are a dev or want to file an Issue, please read [CONTRIBUTING](CONTRIBUTING.md).

## Local setup

### Install toolchain

1. Clone this repo and navigate to it.
   - `git clone https://github.com/Public-Defense-Tech/public-defense-data`
   - `cd public-defende-data`
2. Optional: Install Pyenv if not already installed ([linux, mac](https://github.com/pyenv/pyenv), or [windows](https://github.com/pyenv-win/pyenv-win)). Run `pyenv install` to get the right Python version

### Setup `venv`

First, you'll need to create a virtual environment, this differs depending on your OS.

On linux/mac

```bash
python -m venv .venv
```

On Windows

```powershell
c:\>Python35\python -m venv c:\path\to\repo\ids # (you can substitute `ids` for any name you want)
```

Next, you'll need to "activate" the venv. You'll need to run this command every time you work in the codebase and tell your IDE which Python environment to use. It will likely default to wherever `python` resolves to in your system path. The specific command you run will depend on both your OS and shell.

On linux/mac

| platform | shell      | Command to activate virtual environment |
| :------- | :--------- | :-------------------------------------- |
| POSIX    | bash/zsh   | $ source <venv>/bin/activate            |
|          | fish       | $ source <venv>/bin/activate.fish       |
|          | csh/tcsh   | $ source <venv>/bin/activate.csh        |
|          | PowerShell | $ <venv>/bin/Activate.ps1               |
| Windows  | cmd.exe    | C:\> <venv>\Scripts\activate.bat        |
|          | PowerShell | PS C:\> <venv>\Scripts\Activate.ps1     |

Source: https://docs.python.org/3/library/venv.html#how-venvs-work

If the above doesn't work, try these instructions for creating and activating a virtual environment:
1. Navigate to your project directory: cd [insert file path]
2. Create a virtual environenment: python -m venv venv
3. Activate the virtual environment: .\venv\Scripts\activate.bat

### Install python dependencies

Using `pip`, install the project dependencies.

```shell
pip install -r requirements.txt
```

## Command-Line Usage

This program utilizes `argparse` to allow users to specify various parameters directly from the command line.

**Basic Usage:**

To run the program with default settings or with a subset of arguments, navigate to src, run main.py using the following format:

]
```bash
python main.py [arguments]
```

* `--counties`: Specifies the counties to process. Multiple counties can be provided, separated by spaces. Example: `python your_script_name.py --counties "CountyA" "CountyB" "CountyC"`
* `--start_date`: Specifies the start date for processing, in YYYY-MM-DD format. Example: `python main.py --start_date 2023-01-01`
* `--end_date`: Specifies the end date for processing, in YYYY-MM-DD format. Example: `python main.py --end_date 2023-12-31`
* `--court_calendar_link_text`: Specifies the court calendar link text. Example: `python main.py --court_calendar_link_text "Calendar Link"`
* `--case_number`: Specifies the case number. Example: `python main.py --case_number "Case12345"`
* `--case_html_path`: Specifies the path to the case HTML file. Example: `python main.py --case_html_path "/path/to/case.html"`
* `--judicial_officers`: Specifies the judicial officers. Example: `python main.py --judicial_officers "Judge Smith, Judge Jones"`
* `--ms_wait`: Specifies the milliseconds to wait (integer). Example: `python main.py --ms_wait 500`
* `--parse_single_file`: A flag to indicate that a single file should be parsed. When present, the program will parse a single file. Example: `python your_script_name.py --parse_single_file`

#### Combining Arguments:
You can combine multiple arguments in a single command. For example:
`python main.py --counties "hays" "tyler" --start_date 2023-06-01 --end_date 2023-06-30 --ms_wait 1000`
This command will process data for "hays" and "tyler" between June 1, 2023, and June 30, 2023, with a 1000 millisecond wait between operations.

#### Example with single file parsing:
`python main.py --case_html_path "/path/to/my_case.html" --parse_single_file`
This command parses the single file found at /path/to/my_case.html.

## Structure of Code

- County Database: A CSV table contains the necessary Odyssey links and version for each county in Texas. One column ("scrape") indicates whether that county should be scraped. Currently, Hays is the default.
- Handler (src/handler): This reads the CSV for the counties to be scraped and runs the following processes for each county. You can also set the start and end date of the parser here.

  - **Scraper** (`src/scraper`): This scrapes all of the judicial officers for each day within the period set in the handler and saves all of the HTML to data/[county name]/case_html.
  - **Parser** (`src/parser`): This parses all of the HTML to separate rows in different tables in a postgres database.

## Structure of Data

Data is parsed according to the schema in models.py in src/parser.
