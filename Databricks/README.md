<img src="../ReadmeImages/NiH.jpg" width="1000">

# Microsoft Next in Health - Azure for Researchers Interactive Workshop 
Toronto - April 8, 2019 

## Import a GitHub Repo to Databricks
1. In your Databricks portal, click on **Home** on the left pane and navigate to your user account. Click on the dropdown icon and select **Import**. 

  ![Import GitHub Repo to Databricks](../ReadmeImages/importGit.jpg "Import from GitHub")

2. Select URL and paste the notebook URL (https://raw.githubusercontent.com/azureinfra/NIH/master/Databricks/predict_heartDisease_from_ehr_data.py), then click on **Import**

  ![Import via URL](../ReadmeImages/url.jpg "Import via URL")
  
## Connect MLflow to the Cluster 
1. To connect MLflow to the cluster, click on **Clusters** from the left pane and select your cluster. 

  ![Select Cluster](../ReadmeImages/mlflowcluster.jpg "Select Cluster")
  
2. Select **Libraries** and click on **Install New**. In the Library Source button list, select **PyPI** and type **mlflow** in the package field. 

  ![Install MLflow](../ReadmeImages/mlflowpypi.jpg "Instal MLflow")
  
  
