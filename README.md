# PostgreSQL Data Management Scripts

This repository contains a collection of scripts for managing data in PostgreSQL. These scripts handle various tasks such as creating tables, loading data from CSV files, and creating materialized views for financial data analysis.

## Table of Contents

- [Tasks](#tasks)
- [Requirements](#requirements)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Tasks

### Task 1: Events Management

#### Script: `creating_events.sh`

- **Objective**: 
  - Re-create the `events` table in the PostgreSQL database.
  - Export data from the `events` table to a CSV file.
  - Insert data from the CSV file back into the `events` table.

#### Usage:
```bash
./creating_events.sh
