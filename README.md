This script reads a newline-delimited file of first and last names 
from an [online dataset](https://github.com/philipperemy/name-dataset)
and loads the values into tables on an arbitrary postgres database.

Database configuration is taken from the environment:
```
export DATABASE_USER=
export DATABASE_PASS=
export DATABASE_HOST=
export DATABASE_PORT=
export DATABASE_SSL=
```

The script will create two tables named ```first_names``` and ```last_names```, 
so long as both are absent from the catalog specified by DATABASE_NAME.

