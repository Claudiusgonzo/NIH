<img src="ReadmeImages/NiH.jpg" width="1000">

# Microsoft Next in Health - Azure for Researchers Interactive Workshop 
Toronto - April 8, 2019 

In this workshop you will learn to 
* Create your Azure subscription 
* Get familiar with Azure Portal 
* Create resources – storage and VMs
* Get familiar and run a ML algorithm with Azure Databricks

## Your Azure Pass
You will be provided with a Free to use Azure Pass account code. Please treat this as REAL MONEY as it does come out of a budget.
* Amount: **$100 USD**
* Duration: **30 days**
* Can be extended by providing a payment instrument 
 
## Requirements
* **DO NOT** redeem promo code with an email account that is attached to an Enterprise Agreement, the pass will not work. Use an email account that has **NOT** been used for Azure or O365.
* Best option is to create an Outlook.com Account to use (can be throwaway)
* Monetary credit **can't be used toward third party services**, premier support or Azure Marketplace and cannot be added to existing subscriptions. 
* Accounts cannot be extended - the duration and monetary credit amount cannot be altered
* If you remove the monetary cap by providing a payment instrument, you will be converted to a 'Pay-as-you-go' account at the end of the offer duration. However, if the account has expired, you will need to create a support ticket to enable conversion.
* The Azure Pass offers are restricted to the following regions: 
  * West US 2 
  * West Central US
  * South Central US
  * East US 
  * West Europe
  * Southeast Asia

## Creating a Microsoft Azure Pass Subscription
Creating an Azure Pass subscription is a two-step process.

### Step 1: Redeeming a Microsoft Azure Pass Promo Code:
Open a browser and navigate to: www.microsoftazurepass.com
It is recommended you close all browsers and open a new In-Private Browser session. Other log-ins can persist and cause errors during the activation step.
Click the start button to get started.

Create an email here if needed: https://signup.live.com 

<img src="ReadmeImages/Redeem1.png" width="600">

Enter your account login information and select “Sign In”.

Click “Confirm” if the correct email address is listed.

<img src="ReadmeImages/Redeem2.png" width="600">

Enter your promo code in the Promo code box and click “Claim Promo Code”.

<img src="ReadmeImages/Redeem3.png" width="600">

It may take up to 5 minutes to process the redemption.

<img src="ReadmeImages/Redeem4.png" width="600">

Click on “Activate” to start setting up your Azure subscription.

<img src="ReadmeImages/Redeem5.png" width="600">

### Step 2: Activate your subscription:
Enter your account information and click “Next”.

<img src="ReadmeImages/Redeem6.png" width="600">

Enter your phone number and click “Next”.

<img src="ReadmeImages/Redeem7.png" width="600">

<img src="ReadmeImages/Redeem8.png" width="600">

It may take a few minutes to process the request.

<img src="ReadmeImages/Redeem9.png" width="600">

<img src="ReadmeImages/Redeem10.png" width="600">

Your subscription is ready, click “Get started” to use Azure services.

<img src="ReadmeImages/Redeem11.png" width="600">

## Get Started 
You’re all set. To can access your subscription again later navigate to Azure Portal: **http://portal.azure.com**

# Azure Databricks 
This quickstart shows how to create an Azure Databricks workspace and an Apache Spark cluster within that workspace. Finally, you learn how to run a Spark job on the Databricks cluster. For more information on Azure Databricks, see [What is Azure Databricks?](https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/azure-databricks/what-is-azure-databricks.md)

In this quickstart, you will learn how to create and Azure Databricks workspace and create a Spark Cluster.

## Create an Azure Databricks workspace

In this section, you create an Azure Databricks workspace using the Azure portal.

1. In the Azure portal, select **All Services** > **Analytics** > **Azure Databricks**.

    ![Databricks on Azure portal](ReadmeImages/AzureDBR.jpg "Databricks on Azure portal")

2. Under **Azure Databricks Service**, provide the values to create a Databricks workspace.

    ![Create an Azure Databricks workspace](ReadmeImages/CreateADBR.jpg "Create an Azure Databricks workspace")

    Provide the following values:
    
    |Property  |Description  |
    |---------|---------|
    |**Workspace name**     | Provide a name for your Databricks workspace        |
    |**Subscription**     | From the drop-down, select your Azure subscription.        |
    |**Resource group**     | Create a new resource group. A resource group is a container that holds related resources for an Azure solution. For more information, see [Azure Resource Group overview](https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/azure-resource-manager/resource-group-overview.md). |
    |**Location**     | Select **Canada Central**. For other available regions, see [Azure services available by region](https://azure.microsoft.com/regions/services/).        |
    |**Pricing Tier**     |  Choose **Trial (Premium – 14-Days Free DBUs)**. For more information on these tiers, see [Databricks pricing page](https://azure.microsoft.com/pricing/details/databricks/).       |

    ![Create an Azure Databricks workspace](ReadmeImages/CreateADBR2.jpg "Create an Azure Databricks workspace")

    Select **Pin to dashboard** and then click **Create**.

4. The workspace creation takes a few minutes. During workspace creation, the portal displays the **Submitting deployment for Azure Databricks** tile on the right side. You may need to scroll right on your dashboard to see the tile. There is also a progress bar displayed near the top of the screen. You can watch either area for progress. 

    ![Databricks deployment tile](https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/azure-databricks/media/quickstart-create-databricks-workspace-portal/databricks-deployment-tile.png "Databricks deployment tile")

You can pin the Azure Databricks workspace after it is created as well.

    ![Pin to Dashboard](ReadmeImages/pin.jpg "Pin Azure Databricks workspace to Dashboard")

## Create a Spark cluster in Databricks

1. Click “Go to Resource” to navigate to Azure Databricks or in the Azure portal, go to the Databricks workspace that you created, and then click **Launch Workspace**.

2. You are redirected to the Azure Databricks portal. From the portal, click **Cluster**.

    ![Databricks on Azure](cluster.jpg "Databricks on Azure")

3. In the **New cluster** page, provide the values to create a cluster.

    ![Create Databricks Spark cluster on Azure](ReadmeImages/Cluster.jpg "Create Databricks Spark cluster on Azure")

    Accept all other default values other than the following:

   * Enter a name for the cluster.
   * For this workshop, create a cluster with **4.0** runtime.
   * Make sure you select the **Terminate after \_\_ minutes of inactivity** checkbox. Provide a duration (in minutes) to terminate the cluster, if the cluster is not being used.
    
     Select **Create cluster**. Once the cluster is running, you can attach notebooks to the cluster and run Spark jobs.

For more information on creating clusters, see [Create a Spark cluster in Azure Databricks](https://docs.azuredatabricks.net/user-guide/clusters/create.html).
