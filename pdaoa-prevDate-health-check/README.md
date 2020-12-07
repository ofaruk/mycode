# PDAOA Monitoring through (Stack Driver) 

The PDAOA monitoring requires running aggregation function (count) on number of PDAOA tables, Queries return counts from following list of tables 
(queries run to return results from table where date is yesterdays date):
  
  1.  "rmg-npd-gbi-coredata.core_transaction_data.pda_gps_data" - to get count for number of devices and number of messages for yesterdays date
  2.  "rmg-npd-gbi-coredata.local_oa_data.collection_on_delivery" - row count for where pdate is yesterdays date
  3.  "rmg-npd-gbi-coredata.local_oa_data.collection_scan" - row count for where pdate is yesterdays date
  4.  "rmg-npd-gbi-coredata.local_oa_data.collection_servicepoint" - row count for where pdate is yesterdays date
  5.  "rmg-npd-gbi-coredata.local_oa_data.delivery_route" - row count for where pdate is yesterdays date
  6.  "rmg-npd-gbi-coredata.local_oa_data.delivery_scan" - row count for where pdate is yesterdays date
  7.  "rmg-npd-gbi-coredata.local_oa_data.delivery_servicepoint" - row count for where pdate is yesterdays date
  8.  "rmg-npd-gbi-coredata.local_oa_data.location" - row count for where pdate is yesterdays date
  9.  "rmg-npd-gbi-coredata.local_oa_data.route_summary" - row count for where pdate is yesterdays date and total number of DOs for yesterdays date
  10. "rmg-npd-gbi-coredata.local_oa_data.servicepoint - row count for where pdate is previous day
  11. "rmg-npd-gbi-coredata.local_oa_data.trip" - row count for where pdate is yesterdays date and total number of devices used for yesterdays date
  12. "rmg-npd-gbi-coredata.local_oa_data.trip_activity" - row count for where pdate is yesterdays date
  13. "rmg-npd-gbi-coredata.local_oa_data.trip_location - row count for where pdate is yesterdays date

To mirror the cloud function to other monitoring environment would require following directory structure

environment
├── npd-monitoring
├── cloud-functions                    
│   ├── main.py                   # cloud function that is scheduled to run every five minutes
│   ├── README.md                 # the documentation for the cloud function
│   ├── requirements.txt          # python libraries required for cloud function
│──files
│   ├── npd-files
│       ├── pdaoa-monitoring.json       # to hold project.dataset.tables  names to be used in cloud function
|   ├── ppd-files
│       ├── pdaoa-monitoring.json       # to hold project.dataset.tables  names to be used in cloud function
|   ├── prd-files
│       ├── pdaoa-monitoring.json       # to hold project.dataset.tables  names to be used in cloud function

Once modules are specified as in npd main.tf this should make the cloud functions run the same way for ppd and prd environments


NOTE: the cloud function is triggered by pubsub topic that is scheduled by cloud scheduler and payload set to pdaoa-monitoring.json file i.e. table names

It is also important to not that the service account for the monitoring project should have role :  "roles/bigquery.dataViewer" 
for coredata project  to be able to run the queries and read results that will be returned as part of cloud function, this is set in main.tf
