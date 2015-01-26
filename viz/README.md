Copyright (c) 2015 Cheng-Han

Forked from Cheng-Han

Files required by Flask microserver, and Redis instance.

.
├── app.py -> Start the Python based Flask Microserver
├── dump.rdb -> Redis dump file
├── README.md
├── rt-provision-32.sh -> Shell script to setup development environement in Vagrant
├── static
│   ├── countyLookup.js -> JavaScript Hash with {key = County ID: value = County Name}
│   └── us.json -> JSON file describing the United States map geometry
└── templates
    └── map.html -> HTML file rendered by the server

