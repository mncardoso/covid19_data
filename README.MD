# Covid-19 API
this is a simple api for covid-19 data that is updated once a day

## How des this work?
I have a raspberry pi that runs a python script every day that scrapes the data from the WHO (World Health Organisation) database and then saves it to a json file per country.

## Where can I see the data?
this data is being use by the [covid19_dashboard](https://github.com/mncardoso/covid19_dashboard) which is a simple dashboard that shows the data graphically using d3.js. You can see the dashboard [here](https://covid.miguel-cardoso.com/)