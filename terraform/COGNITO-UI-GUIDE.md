# Using Cognito UI to Manage Users

## ✅ Yes! You Can Use Cognito UI

AWS Cognito provides a **web-based UI in the AWS Console** for managing users. This is perfect for:
- Creating users
- Assigning tenants
- Resetting passwords
- Viewing user details
- Managing user attributes

## Accessing Cognito UI

### Step 1: Get Your User Pool ID

```bash
# From Terraform
terraform output cognito_user_pool_id

# Or from AWS CLI
aws cognito-idp list-user-pools --max-results 10
```

### Step 2: Open Cognito Console

1. Go to **AWS Console** → **Cognito** → **User Pools**
2. Click on your user pool: `bagel-prod-users`
3. You'll see the user management interface

**Direct URL format:**
```
https://console.aws.amazon.com/cognito/v2/idp/user-pools/{USER_POOL_ID}/users
```

Replace `{USER_POOL_ID}` with your actual pool ID.

## Managing Users in Cognito UI

### Create a New User

1. **Click "Create user"** button
2. **Enter user details:**
   - **Username**: `john@acme.com` (or email)
   - **Email**: `john@acme.com`
   - **Temporary password**: (auto-generated or set manually)
   - **Mark email as verified**: ✅ (recommended)
3. **Click "Create user"**

### Assign Tenant to User

1. **Click on the user** in the list
2. **Go to "Attributes" tab**
3. **Click "Edit"**
4. **Add custom attribute:**
   - **Name**: `custom:tenant_id`
   - **Value**: `acme-corp` (your tenant name)
5. **Click "Save changes"**

### View User Details

1. **Click on user** in the list
2. **View tabs:**
   - **Attributes**: Email, tenant_id, etc.
   - **Groups**: User groups (if any)
   - **Devices**: Registered devices
   - **Activity**: Sign-in history
   - **Risk**: Security events

### Reset User Password

1. **Click on user**
2. **Click "Reset password"** button
3. **Choose:**
   - **Send email** (if email sending configured)
   - **Generate temporary password** (copy and share)
4. User will need to change password on next login

### Disable/Enable User

1. **Click on user**
2. **Click "Disable user"** or **"Enable user"**
3. Disabled users cannot sign in

### Delete User

1. **Click on user**
2. **Click "Delete user"** button
3. **Confirm deletion**
   - ⚠️ This is permanent!

### Bulk Operations

**Import Users (CSV):**
1. **Click "Users"** → **"Import users"**
2. **Upload CSV file** with format:
   ```
   username,email,email_verified,custom:tenant_id
   john@acme.com,john@acme.com,true,acme-corp
   jane@widget.com,jane@widget.com,true,widget-inc
   ```
3. **Click "Import"**

**Export Users:**
1. **Click "Users"** → **"Export users"**
2. **Choose format**: CSV or JSON
3. **Download** user list

## Tenant Management Workflow

### Complete User Onboarding via UI

1. **Create User:**
   - Username: `john@acme.com`
   - Email: `john@acme.com`
   - Temporary password: (auto-generated)

2. **Assign Tenant:**
   - Go to user → Attributes → Edit
   - Add: `custom:tenant_id` = `acme-corp`
   - Save

3. **Verify:**
   - Check Attributes tab shows tenant_id
   - User can now sign in and access tenant data

### Bulk Tenant Assignment

**Option 1: Via CSV Import**
```csv
username,email,email_verified,custom:tenant_id
user1@acme.com,user1@acme.com,true,acme-corp
user2@acme.com,user2@acme.com,true,acme-corp
user3@widget.com,user3@widget.com,true,widget-inc
```

**Option 2: Via Groups**
1. Create group: `tenant-acme-corp`
2. Add users to group
3. Use Lambda trigger to map group → tenant_id

## UI Features Available

### User List View

- ✅ **Search users** by email, username
- ✅ **Filter** by status, groups
- ✅ **Sort** by name, created date
- ✅ **Bulk actions** (enable, disable, delete)

### User Detail View

- ✅ **Attributes**: All user attributes including `custom:tenant_id`
- ✅ **Groups**: User group memberships
- ✅ **Devices**: Registered MFA devices
- ✅ **Activity**: Sign-in history and events
- ✅ **Risk**: Security and risk events

### User Actions

- ✅ **Create user**
- ✅ **Edit attributes** (including tenant_id)
- ✅ **Reset password**
- ✅ **Disable/Enable user**
- ✅ **Delete user**
- ✅ **Send verification email**
- ✅ **Add to groups**

## Tenant Assignment Best Practices

### Method 1: Manual Assignment (UI)

**Best for**: Small scale, ad-hoc assignments

1. Create user in UI
2. Edit attributes
3. Add `custom:tenant_id`
4. Save

### Method 2: CSV Import (Bulk)

**Best for**: Bulk onboarding

1. Prepare CSV with tenant assignments
2. Import via UI
3. Verify tenant attributes

### Method 3: Groups → Tenant Mapping

**Best for**: Organization-based tenants

1. Create group: `tenant-acme-corp`
2. Add users to group via UI
3. Use Lambda trigger to auto-assign tenant_id from group

## Quick Reference: Common Tasks

### Create User with Tenant

```
1. Users → Create user
2. Enter: email, username
3. Create
4. Click user → Attributes → Edit
5. Add: custom:tenant_id = "acme-corp"
6. Save
```

### Update Tenant Assignment

```
1. Users → Find user
2. Click user → Attributes → Edit
3. Change: custom:tenant_id = "new-tenant"
4. Save
```

### Reset Password

```
1. Users → Find user
2. Click user
3. Actions → Reset password
4. Choose: Send email or Generate temp password
```

### Verify Tenant Assignment

```
1. Users → Find user
2. Click user → Attributes
3. Look for: custom:tenant_id
4. Should show: "acme-corp" (or your tenant)
```

## Limitations

### What You CAN Do in UI:
- ✅ Create, edit, delete users
- ✅ Set/change tenant_id attribute
- ✅ Reset passwords
- ✅ Enable/disable users
- ✅ View user activity
- ✅ Import/export users

### What You CANNOT Do in UI:
- ❌ Automatically map email domain → tenant (use Lambda trigger)
- ❌ Bulk edit tenant assignments (use CSV import or script)
- ❌ Set up Google OAuth (use Terraform/CLI)

## Integration with Scripts

You can combine UI management with scripts:

```bash
# Create user in UI, then verify with script
./terraform/scripts/verify-tenant.sh john@acme.com

# Or create via script, manage in UI
./terraform/scripts/onboard-user.sh john@acme.com acme-corp
# Then use UI to reset password, view activity, etc.
```

## Security Considerations

### Access Control

- **IAM Permissions**: Users accessing Cognito UI need:
  ```json
  {
    "Effect": "Allow",
    "Action": [
      "cognito-idp:AdminCreateUser",
      "cognito-idp:AdminUpdateUserAttributes",
      "cognito-idp:AdminGetUser",
      "cognito-idp:ListUsers"
    ],
    "Resource": "arn:aws:cognito-idp:*:*:userpool/*"
  }
  ```

### Audit Trail

- All actions in Cognito UI are logged in **CloudTrail**
- View who created/modified users
- Track tenant assignment changes

## Summary

✅ **Yes, you can use Cognito UI to manage users!**

**What you can do:**
- ✅ Create users
- ✅ Assign tenants (via `custom:tenant_id` attribute)
- ✅ Reset passwords
- ✅ View user details
- ✅ Manage user attributes
- ✅ Import/export users
- ✅ Enable/disable users

**Workflow:**
1. **Create user** in Cognito UI
2. **Assign tenant** via Attributes tab
3. **User signs in** (email/password or Google)
4. **Gets JWT token** with tenant_id
5. **System uses tenant** for S3 paths and billing

The UI is perfect for day-to-day user management, while scripts are great for bulk operations! 🎉
