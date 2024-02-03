# BOM_PROGRAM_CREATE
To automate BOM program creation, generated based on master BOM program. Developed by [jiajunlee](https://github.com/jiajunlee19).
- The current [bom-program-create](https://github.com/jiajunlee19/bom-program-create) project is for python-developer only. If you wish to continue, installation are provided in below section.
- For non-python user, please navigate to the alternative [bom-program-create-app](https://github.com/jiajunlee19/bom-program-create-app) solution.

<br>

![flowchart.png](Misc/flowchart.png)

<br>

### Installation
- Fork this project [here](https://github.com/jiajunlee19/bom-program-create/fork)
<br><br>
    OR
<br><br> 
- Clone this project by `git clone "https://github.com/jiajunlee19/bom-program-create.git"`
<br><br>
    OR
<br><br> 
- Download this project in zip [here](https://github.com/jiajunlee19/bom-program-create/archive/refs/heads/master.zip) and extract

<br><br> 
- Note that Python is required to run, below step will help to install the required modules:
    1. Install Python, if you have not install it yet
    2. Launch `cmd` (command prompt), run `cd "C:\Downloads\bom-program-create"`, replace with whichever path the project is installed on
        - Run `pip install --upgrade -r requirements.txt`
        - Run `set Path=%Path%,%python -m site --user-base%`

<br>

### How to run?
0. (Optional) Go to `settings` sheet in [BOM.xlsx](BOM.xlsx), modify the settings if needed.
    - If SAP_SOURCE = `db`, database settings need to be configured in `settings.py`, refer to [settings.template.py](settings.template.py).
1. Go to `BOM` sheet in [BOM.xlsx](BOM.xlsx), fill in master and new BOM info accordingly.
2. (For SAP_SOURCE = `manual` selected in step `#0` only): Place all relevant BOM and MCTO files in `BOM_590` folder and `MCTO` folder.
    - The provided files must be in `.csv` format exported from `SAP`.
    - For MCTO, the files must be named as `MCTO`_`PV` (eg: `705043_1.csv`)
3. Place all relevant bom recipes in `recipe-bom-master` folder.
4. Run [main.py](main.py).
5. Created new BOM recipes can be found in `recipe-bom-new` folder within subfolder grouped by `BOM_MCTO_PV`.
6. (Optional): View the BOM differences detail in `SCRIPT_OUTPUT.xlsx` by subfolder in `recipe-bom-new`.
7. (Optional): View the logs in [Log/BOM_PROGRAM_CREATE.log](Log/BOM_PROGRAM_CREATE.log).

<br>

### Limitations
Below are the limitations that cannot be handled. Warning will be raised as such the impacted line item will be skipped.
1. `Part Extra Place` or `Adding Part` cannot be handled.
2. `Part Sub` or `Subbing Part` with feeder used in any non-impacted designator cannot be handled.

<br>

### Test Cases
Below are the passing test cases, using [590-624661](BOM_590/590-624661.csv) as Master BOM.
References: [BOM_590](BOM_590/) and [recipe-bom-new](recipe-bom-new/)
| New BOM                              | Differences from Master BOM | Result                                                   |
| :---                                 | :-------------------------  | :----                                                    |
| [590-624662](BOM_590/590-624662.csv) | No differences              | Generated new recipes, exactly same with master recipes. |
| [590-624663](BOM_590/590-624663.csv) | Adding Part (R1000,R2000) into 510-500881              | No new recipes generated as per limitation `#1`          |
| [590-624664](BOM_590/590-624664.csv) | Removing All Part from 510-500881                | Generated new recipes as expected.                       |
| [590-624665](BOM_590/590-624665.csv) | Removing Partial Part (R69,R91,R107,R112-R113) from 510-500881              | Generated new recipes as expected.                       |
| [590-624666](BOM_590/590-624666.csv) | Subbing All Part from 510-500881 to 510-500882              | Generated new recipes as expected.                       |
| [590-624667](BOM_590/590-624667.csv) | Subbing Partial Part (R69,R91,R107,R112-R113,R120) from 510-500881 to 510-500882              | No new recipes generated as per limitation `#2`          |

<br>

### Details
Here are some details if you are interested on how the `.pp` or `.pp7` are modified and being generated into `recipe-bom-new`.

0. Master BOM and New BOM Info
    <br>
    ![Master BOM and New BOM Info.png](Misc/0.%20Master%20BOM%20and%20New%20BOM%20Info.PNG)
    <br>
    ![Place BOM recipes.png](Misc/0.%20Place%20BOM%20recipes.PNG)

<br>

1. Part Removal - Delete Component
    <br>
    ![Part Removal - Delete Component.png](Misc/1.%20Part%20Removal%20-%20Delete%20Component.PNG)

<br>

2. Part Removal - Delete Feeder
    <br>
    ![Part Removal -Delete Feeder.png](Misc/2.%20Part%20Removal%20-%20Delete%20Feeder.PNG)

<br>

3. Part Removal - Delete Pick to Place
    * For pp
        <br>
        ![Part Removal - Delete Pick to Place.png](Misc/3.%20Part%20Removal%20-%20Delete%20Pick%20to%20Place.PNG)
        <br>
    * For pp7
        <br>
        ![Part Removal - Delete Pick to Place pp7.png](Misc/3.%20Part%20Removal%20-%20Delete%20Pick%20to%20Place%20pp7.PNG)

<br>

4. Part Sub - Modify Component
    <br>
    ![Part Sub - Modify Component.png](Misc/4.%20Part%20Sub%20-%20Modify%20Component.PNG)

<br>

5. Part Sub - Modify Feeder
    <br>
    ![Part Sub - Modify Feeder.png](Misc/5.%20Part%20Sub%20-%20Modify%20Feeder.PNG)

<br>

6. Generated New BOM Recipes
    <br>
    ![Generated BOM Recipes.png](Misc/6.%20Generated%20BOM%20Recipes.PNG)

<br>