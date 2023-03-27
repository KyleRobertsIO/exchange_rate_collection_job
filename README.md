# **Exchange Rate Collection API**

This small python application leverages the free 
web service [exchangerate.host](https://exchangerate.host/)
for collect exchanges rates for a given global currency.

The job code currently only supports Postgres as a target database
for save the data.

The job code can run in `HISTORICAL` or `NIGHTLY` mode when collecting
data. HISTORICAL allows you to go back a few days from your specified 
end date. NIGHTLY will allow you to setup a daily cron job to collect new
rate records for whatever application you would need it for.

## **Getting Started**

To setup the environment for the application run the `setup.sh` script file.

When the .env file is created you will want to fill in your particular config requirements.

Run the job with command below...

```sh
python main
```

## **Postgres Table**

Use the below create table statement inside of your target Postgres instance
to properly integrate with the job code.

```sql
CREATE TABLE IF NOT EXISTS dbo.exchange_rates (
    id SERIAL PRIMARY KEY NOT NULL,
    date DATE NOT NULL,
    rates JSONB NOT NULL,
    source VARCHAR(18) NOT NULL
);
```