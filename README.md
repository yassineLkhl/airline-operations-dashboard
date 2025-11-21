# airline-operations-dashboard
Public overview of a flight operations dashboard originally built in Palantir Foundry, with mock data, screenshots and a simplified analysis pipeline.

## Project Context

This repository provides a public overview of a flight operations analytics project originally developed inside Palantir Foundry as part of a hands-on workshop.

Within Foundry, the project included:

- a data pipeline built using Python and PySpark
- several cleaning, joining, and enrichment steps
- a visual data lineage graph showing the transformation flow
- an interactive dashboard combining filters, charts, a table, and a detailed object view

Because the original environment and datasets are not publicly accessible, this repository includes mock data, screenshots, and documentation to illustrate the work performed in Foundry.

## Repository Structure
This repository will be organized into the following folders as the project is documented:

- `data/` – mock flight dataset inspired by the structure of the original Foundry tables.
- `code/` – optional PySpark transformation scripts illustrating the cleaning and enrichment logic.
- `screenshots/` – captures of the Foundry dashboard, pipeline, and data lineage.
- `docs/` – additional documentation such as diagrams or data dictionaries.

This structure reflects the main elements of the original Foundry project: data transformations, dashboard components, and lineage.

## Data Pipeline Overview
The original project in Palantir Foundry included a data pipeline implemented using Python and PySpark within the Transform framework.  
The pipeline performed several processing steps on the flight-related datasets, including:

- cleaning and standardizing raw tables
- dropping unused or system-generated columns
- joining flights with carriers and airports
- enriching the dataset to produce a consolidated view used in the dashboard

The pipeline was fully traceable through Foundry's data lineage interface, which provided a visual representation of how each transformation stage contributed to the final dataset.

Screenshots of the pipeline and lineage view will be added in the `screenshots/` directory to illustrate the structure and execution flow.

## Dashboard Overview
The dashboard created in Palantir Foundry provided an interactive exploration interface for flight operations data.  
It included dynamic filters, visual charts, a detailed table, and a contextual object view linked to the selected flight.

### Filters

The dashboard included the following interactive filters:

- Carrier  
- Destination (Dest)  
- Flight Date  
- Arrival Delay  
- Origin  
- Tail Number  

All filters were connected to a filtered dataset ("Filtered Data") used across all components of the dashboard.

### Visualizations

Two visual components were built using the filtered dataset:

1. **Average Arrival Delay by Carrier**  
   Bar chart with:  
   - X axis: Carrier  
   - Y axis: Average arrival delay  

2. **Flight Count by Destination**  
   Pie chart grouped by destination with a count aggregation.

### Table View

A table displayed the list of flights after filtering.  
The main columns included:

- Title  
- Flight Id  
- Flight Number  
- Tail Number  
- Carrier  
- Destination  
- Arrival Delay  
- Origin  

### Object View

Selecting a row in the table displayed additional details for the chosen flight, including:

- Carrier  
- Destination  
- Flight Number  
- Flight Date  
- Tail Number  
- Origin  
- Departure Delay  
- Arrival Delay  

The object view also exposed links to related airport and carrier entities.

### Export Function

A custom export button allowed downloading the filtered dataset as an Excel file.  
The export was based on the same filtered object set used across the dashboard.

### Layout

The final layout consisted of:

- Top row: filter list on the left, export button on the right  
- Middle row: bar chart on the left, pie chart on the right  
- Bottom row: table on the left, object view on the right

### Screenshots

Screenshots of each dashboard section will be added to the `screenshots/` folder and referenced here:

- Dashboard overview  
- Filters panel  
- Bar chart  
- Pie chart  
- Table view  
- Object view  
- Export button

## Mock Dataset
(To be completed.)

## Example Transformations
(To be completed.)

## Screenshots
(To be completed.)
