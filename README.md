# eGo_app_demand
The app employs the oemof demandlib to generate demand time series for open_eGo
project.

This app won't do sophisticated things ;-) It relies on the oemof.db
`dev` branch.

There is not installation required to use this app. Just call the file by
`eGo_app_demand.py` with its according CLI arguments.
An example to generate a new database table containing peak demands for defined
load areas is

```
./eGo_app_demand.py peak_load
```

if this should only by done for the test area Swabia (Bavaria) call the app as
follows

```
./eGo_app_demand.py peak_load -t rli_deu_loadarea_spf
```

or call

```
./eGo_app_demand.py --help
```

to get further help.

