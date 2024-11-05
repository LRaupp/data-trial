### Clever Data-Trial Project Summary
[![Watch the video](https://raw.githubusercontent.com/LRaupp/data-trial/main/docs/dashboard-demo-image.png)](https://raw.githubusercontent.com/LRaupp/data-trial/main/docs/dashboard-demo.mp4)
*Click on the image above to watch the dashboard demo video.*

- **Connectivity and Error Resolution**
  - Fixed connectivity issues between Docker containers.
  - Resolved import and timeout errors within the main DAG.
  - Adjustments to the CSV reading process to prevent data loss.

- **Data Processing and Transformation**
  - Developed a new DAG incorporating Spark tasks, simulating large-scale transformations.
  - Normalized state and city columns in data frames (dfs) to maintain consistency and facilitate accurate analysis.

- **Dashboard Development**
  - **Interactive Dashboard**: Developed an interactive dashboard that dynamically updates data based on user input selections using plotly + dash.

  - **Analyses Processed by the Dashboard**:
    - **Join Operations**: Integrated Google reviews with company profiles to provide comprehensive insights.
    - **Dynamic Filters**: Implemented filters for city and state to allow users to refine their analysis based on locality.
    - **Date Range Filter**: Enabled filtering by a specified date range to analyze data over different time periods.
    - **Rating Metrics**: Calculated the average, minimum, maximum, and count of ratings, with options to sort results by average rating and count.
    - **Geolocation**: Displayed the geolocation of companies on a map to visualize their distribution.
    - **Regional Insights**: Provided a total count of companies within the selected region.
    - **Category Count**: Calculated the number of categories available in the selected area.
    - **Company-Specific Analysis**: For a selected company, calculated:
      - Monthly average rating over the specified date range.
      - Examples of reviews for qualitative insights.
  
  - **Data Integration**: Utilized the normalization of state and city data performed during the Airflow process to include FMCSA data in the dashboard, allowing for consistent filtering:
    - **Joins with FMCSA Tables**: Joined `public.fmcsa_companies` and `public.fmcsa_complaints` for enriched data analysis.
    - **FMCSA Filtering**: Implemented filtering of FMCSA companies by city and state, with sorting options by date and complaint volume for each topic.
    - **Complaint Counting**: Counted the number of complaints for FMCSA companies within the specified period.

  - **Running the Dashboard**:
    - Run the DAG called `clever_spark_DAG`
    - Execute the dashboard, run the notebook located at: `./notebooks/company_reports.ipynb`.
    - Access the dashboard at: [http://localhost:8050](http://localhost:8050).
