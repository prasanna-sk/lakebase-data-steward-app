# 📋 Setup Checklist

## Step 0: Get the Source Code ✅

### ✅ Clone Repository
- [ ] Clone the GitHub repository:
  ```bash
  git clone https://github.com/prasanna-sk/lakebase-data-steward-app.git
  cd lakebase-data-steward-app
  ```
- [ ] Verify all files are present (app.py, app.yaml, requirements.txt, README.md)

## Step 1: Create Lakebase Database Instance ✅

### ✅ Lakebase Instance Creation
- [ ] Go to **Data** > **Create** > **Lakebase Database** in Databricks workspace
- [ ] Choose a name for your instance (e.g., `sales-data-lake`, `customer-mdm`)
- [ ] Configure settings as needed
- [ ] **Note down the instance name and instance ID** after creation
- [ ] **Note down the Lakebase hostname** from connection details

## Step 2: Set Up Database Resources ✅

### ✅ Database Setup
> **⚠️ IMPORTANT**: PostgreSQL support in Databricks SQL Editor is LIMITED. Follow step-by-step approach from **README.md Step 2**:

- [ ] **Create Schema**:
  - [ ] Click "New Query" (top right) after creating Lakebase instance
  - [ ] Copy/paste and execute the `CREATE SCHEMA` command from README.md Step 2.1

- [ ] **Change Schema Context**:
  - [ ] Manually change schema dropdown to: `databricks_postgres.your_schema_name`

- [ ] **Create Tables** (from README.md Step 2.3 - execute one at a time):
  - [ ] ✅ `customer_info` table
  - [ ] ✅ `locations` table
  - [ ] ✅ `products` table
  - [ ] ✅ `data_steward_audit` table

- [ ] **Insert Sample Data** (from README.md Step 2.4 - optional):
  - [ ] ✅ Customer sample data
  - [ ] ✅ Location sample data
  - [ ] ✅ Product sample data
  - [ ] ❌ **SKIP PERMISSIONS** - we'll do this in Step 6

## Step 3: Initial App Configuration ✅

### ✅ Update app.yaml with Lakebase Instance Details
- [ ] Update `app.yaml` with your environment variables (see **README.md Step 3** for complete reference):
  - [ ] `LAKEBASE_INSTANCE_NAME` - Your Lakebase instance name (from Step 1)
  - [ ] `LAKEBASE_INSTANCE_ID` - Your Lakebase instance ID (from Step 1)
  - [ ] `PGDATABASE` - Set to `databricks_postgres` (database name when Lakebase is added as app resource)
  - [ ] `PGUSER` - Set to `placeholder-service-principal` (temporary)
  - [ ] `PGHOST` - Your Lakebase instance hostname
  - [ ] `PGPORT` - Set to `5432` (NOT 443)
  - [ ] `PGAPPNAME` - Set to your Databricks app name (same as what you'll use in deploy command)
  - [ ] `DEFAULT_SCHEMA` - Your schema name (from Step 2)
  - [ ] `APP_TITLE` - Your desired app title
  - [ ] `COMPANY_NAME` - Your company name
  - [ ] `LOGO_URL` - Your logo URL (optional)

## Step 4: Initial Deployment (to get Service Principal) ✅

### ✅ First Deployment
- [ ] **⚠️ PREREQUISITE**: Ensure Databricks CLI is authenticated (see [Authentication Guide](https://docs.databricks.com/en/dev-tools/cli/authentication.html))
- [ ] Deploy the app (choose Option A or B):

**Option A: CLI Deployment**
- [ ] Deploy via CLI:
  ```bash
  databricks apps deploy your-app-name --source-code-path .
  ```
  > **Note**: Replace `your-app-name` with the same name you used for `PGAPPNAME` in your app.yaml

**Option B: UI Deployment**
- [ ] Go to **Databricks Apps** in your workspace
- [ ] Click **Create App**
- [ ] Upload your source code (app.py, app.yaml, requirements.txt)
- [ ] **IMPORTANT**: In **App Resources** section, add your **Lakebase instance** as a resource
- [ ] Deploy the app

- [ ] Verify deployment success ✅
- [ ] Get the service principal ID:
  ```bash
  databricks apps get your-app-name
  ```
- [ ] **Note down the service principal ID** - you'll need this for Step 5

### ✅ Expected Behavior After Step 4
- [ ] App is deployed and accessible
- [ ] App shows permission errors in logs (this is normal and expected)
- [ ] Schema/table dropdowns may be empty (will work after Step 6)

## Step 5: Update App with Real Service Principal and Redeploy ✅

### ✅ Update app.yaml with Real Service Principal
- [ ] Update `app.yaml` with the real service principal ID from Step 4:
  - [ ] Replace `PGUSER` value with actual service principal ID
- [ ] Redeploy the application (choose Option A or B):

**Option A: CLI Redeploy**
- [ ] Redeploy via CLI:
  ```bash
  databricks apps deploy your-app-name --source-code-path .
  ```

**Option B: UI Redeploy**
- [ ] Go to your existing app in **Databricks Apps**
- [ ] Upload the updated `app.yaml` file
- [ ] Click **Deploy** to redeploy with the new service principal ID

- [ ] Verify redeploy success ✅

## Step 6: Grant Permissions ✅

### ✅ Execute Permission Grants (from README.md Step 6)
- [ ] Replace placeholders with actual values:
  - [ ] `your_schema_name` → your actual schema name
  - [ ] `your-service-principal-id` → actual service principal ID from Step 4

- [ ] Copy/paste and execute the GRANT statements from **README.md Step 6** one at a time:
  - [ ] ✅ Schema permissions GRANT
  - [ ] ✅ Table permissions GRANT
  - [ ] ✅ Sequence permissions GRANT

### ✅ Verification
- [ ] App now works without permission errors
- [ ] Schema dropdown populates with your schema
- [ ] Table dropdown shows your tables when schema is selected

## Final Verification ✅

### ✅ App Functionality Test
- [ ] Open the application URL
- [ ] Verify branding appears correctly (logo, title, company name)
- [ ] Select your schema from the dropdown
- [ ] Verify tables appear in the table dropdown
- [ ] Select a table and verify data loads
- [ ] Test basic operations:
  - [ ] View existing data ✅
  - [ ] Add a new row ✅
  - [ ] Edit existing data ✅
  - [ ] Delete a row ✅
  - [ ] Search functionality ✅
  - [ ] Filter by status ✅
  - [ ] View audit trail ✅

## 🎉 Success!

Your Lakebase Data Stewardship Portal is now ready for production use!

## 🚨 Troubleshooting

### If Step 4 (Initial Deployment) Fails:
- Check that all database resources from Step 2 exist
- Verify your `app.yaml` configuration
- Check Databricks Apps deployment logs

### If Step 5 (Redeploy) Fails:
- Verify you're using the correct service principal ID from Step 4
- Check that your `app.yaml` was updated properly
- Check Databricks Apps deployment logs

### If Step 6 (Permissions) Doesn't Work:
- Verify you're using the correct service principal ID from Step 4
- Check that schema and table names match exactly
- Ensure you have permission to grant permissions in your Lakebase instance

### If App Still Shows Errors:
- Check app logs in Databricks Apps console
- Verify environment variables are set correctly
- Test database connectivity from SQL editor

---

**📝 Key Advantage of This Approach:**
- ✅ Clean, logical sequence
- ✅ Solves the service principal chicken-and-egg problem
- ✅ Deploy → Get service principal → Update config → Redeploy → Grant permissions
- ✅ Clear error boundaries (Step 4 = initial deployment, Step 5 = redeploy, Step 6 = permissions)
- ✅ Working app after Step 6 with proper audit logging