🚀 STEP 1: CREATE S3 BUCKET
🔹 Go to AWS Console → S3
🔹 Create bucket: nhl-data-bucket
🔹 Keep default settings (no need public access)


🔐 STEP 2: CREATE IAM ROLE FOR LAMBDA
🔹 Go to IAM → Roles → Create Role (lambda-nhl-role)
🔹 Attach policies:
                    AmazonS3FullAccess
                    AmazonSESFullAccess
                    SecretsManagerReadWrite
                    AWSLambdaBasicExecutionRole


📧 STEP 3: SETUP AWS SES (VERY IMPORTANT)
🔹 Go to SES → Verify Email Address
🔹 Add your sender email
🔹 Verify it from inbox
🔹 Verify recipient email


❄️ STEP 4: SNOWFLAKE SETUP
    🔹 Create Main Table:
        CREATE OR REPLACE TABLE NHL_RAW
        (
        Team STRING,
        Year INT,
        Wins INT,
        Losses INT,
        OT_Losses INT,
        Win_PCT FLOAT,
        GF INT,
        GA INT,
        GD INT
        );
    
    🔹 Create Winner Table:
        CREATE OR REPLACE TABLE NHL_WINNERS AS
        SELECT *
        FROM
        (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Wins DESC) rn
            FROM NHL_RAW
        )
        WHERE rn = 1;


🧩 STEP 5: LAMBDA FUNCTION FULL CODE
Main file --> C:\Users\maity\OneDrive\Documents\VS Code\AWS Lambda\nhl-pipeline\lambda_function.py


🚀 STEP 6: STORE SECRETS IN AWS SECRETS MANAGER
🔹 Go to: AWS Console → Secrets Manager → Store a new secret
    Secret Type-- Other type of secret
    Secret Name-- nhl-pipeline-secret
    Add key-value pairs in JSON:
        {
        "snowflake_user": "xxx",
        "snowflake_password": "xxx",
        "snowflake_account": "xxx",
        "snowflake_warehouse": "xxx",
        "snowflake_database": "xxx",
        "snowflake_schema": "PUBLIC",
        "s3_bucket": "your-bucket-name",
        "sender_email": "verified@example.com",
        "recipient_email": "receiver@example.com"
        }


🚀 STEP 7: Prepare Deployment Package
🔹 Zip the main Python file (lambda_function.py) for Lambda function--> C:\Users\maity\OneDrive\Documents\VS Code\AWS Lambda\nhl-pipeline\Lambda-function_v3.zip


🚀 STEP 8: Create Lambda Layer (for dependencies)
🔹 Install dependencies locally:
    pip install --platform manylinux2014_x86_64 --target=python --implementation cp --python-version 3.10 --only-binary=:all: --upgrade snowflake-connector-python requests beautifulsoup4
🔹 Once the installation finishes, right-click that "python" folder and compress it into a .zip
    File--> C:\Users\maity\OneDrive\Documents\VS Code\AWS Lambda\nhl-pipeline\python-layer-lambda.zip
    This will be used as the "Layer" for the Lambda function.
🔹 Upload Layer
    Go to AWS → Lambda → Layers → Create layer
    Fill:
    Name: nhl-deps-layer  ## Actual Used layer--> nhl-deps-layer-linux-v2
    Upload: python-layer-lambda.zip
    Runtime: Python 3.10
🔹 Attach Layer
    Open Lambda function
    Scroll → Layers
    Click → Add a layer


🚀 STEP 9: Create Lambda Function
🔹 Go to: AWS Console → Lambda → Create Function
🔹 Configure:
        Name- nhl-pipeline
        Runtime- Python 3.10
        Execution Role- lambda-nhl-role
🔹 Attach Layer- nhl-deps-layer
🔹 Upload code zip (Lambda-function_v3.zip)


🚀 STEP 10: Test the Lambda
🔹 Test → Create new event
🔹 Use JSON as: {}
🔹 Click Test


🚀 STEP 11: Verify below
🔹 Go to: Lambda → Code tab → Runtime settings
🔹 Check: Handler = lambda_function.lambda_handler
🔹 CONFIGURE TIMEOUT & MEMORY:
    Set:
        Timeout → 10 minutes
        Memory → 512 MB (or 1024 MB safer)  -- As reuired