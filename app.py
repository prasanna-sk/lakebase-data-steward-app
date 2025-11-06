import dash
from dash import html, dcc, Input, Output, State, callback_context
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import psycopg
import os
import time
import pandas as pd
import json
from databricks import sdk
from psycopg import sql
from psycopg_pool import ConnectionPool

# Database connection setup
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None

def refresh_oauth_token():
    """Refresh OAuth token if expired."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        try:
            postgres_password = workspace_client.config.oauth_token().access_token
            last_password_refresh = time.time()
        except Exception as e:
            print(f"❌ Failed to refresh OAuth token: {str(e)}")
            return False
    return True

def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')} "
            f"application_name={os.getenv('PGAPPNAME')}"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
    return connection_pool

def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    
    # Recreate pool if token expired
    if postgres_password is None or time.time() - last_password_refresh > 900:
        if connection_pool:
            connection_pool.close()
            connection_pool = None
    
    return get_connection_pool().connection()

# Configuration variables from environment
DEFAULT_SCHEMA = os.getenv('DEFAULT_SCHEMA', 'your_schema_name')
APP_TITLE = os.getenv('APP_TITLE', 'MDM - Data Stewardship Portal')
COMPANY_NAME = os.getenv('COMPANY_NAME', 'Your Company')
LOGO_URL = os.getenv('LOGO_URL', 'https://cdn.bfldr.com/9AYANS2F/at/xmmrtc5gg9vctkpj3t6fxvj8/primary-icon-navy-data-quality-3.svg?auto=webp')

def get_available_schemas():
    """Get list of schemas user has access to."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schema_name
                """)
                schemas = cur.fetchall()
                return [{"label": schema[0], "value": schema[0]} for schema in schemas]
    except Exception as e:
        print(f"Error getting schemas: {e}")
        return [{"label": DEFAULT_SCHEMA, "value": DEFAULT_SCHEMA}]

def get_available_tables(schema_name=None):
    """Get list of available tables in the selected schema."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # First, try to find tables with multiple approaches for better compatibility
                queries_to_try = [
                    # Standard query with BASE TABLE filter
                    ("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """, "with BASE TABLE filter"),

                    # More permissive query without table_type filter
                    ("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        ORDER BY table_name
                    """, "without table_type filter"),

                    # Try with lowercase schema name
                    ("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE LOWER(table_schema) = LOWER(%s)
                        ORDER BY table_name
                    """, "with case-insensitive schema"),
                ]

                tables = []
                last_error = None

                for query, description in queries_to_try:
                    try:
                        print(f"Trying table discovery {description} for schema: {schema_name}")
                        cur.execute(query, (schema_name,))
                        tables = cur.fetchall()

                        if tables:
                            print(f"Found {len(tables)} tables using {description}")
                            break
                        else:
                            print(f"No tables found using {description}")
                    except Exception as query_error:
                        last_error = query_error
                        print(f"Failed {description}: {query_error}")
                        continue

                # If no tables found, try diagnostic query
                if not tables:
                    try:
                        print(f"No tables found. Running diagnostic query...")
                        cur.execute("""
                            SELECT table_schema, table_name, table_type
                            FROM information_schema.tables
                            ORDER BY table_schema, table_name
                        """)
                        all_tables = cur.fetchall()
                        print(f"All available tables in database: {all_tables}")

                        cur.execute("""
                            SELECT DISTINCT table_schema
                            FROM information_schema.tables
                            ORDER BY table_schema
                        """)
                        all_schemas = cur.fetchall()
                        print(f"All available schemas: {all_schemas}")

                    except Exception as diag_error:
                        print(f"Diagnostic query failed: {diag_error}")

                table_list = []
                for table_name, table_type in tables:
                    # Special formatting for audit table
                    if 'audit' in table_name.lower():
                        label = f"📋 {table_name.replace('_', ' ').title()}"
                    else:
                        label = table_name.replace('_', ' ').title()

                    table_list.append({"label": label, "value": table_name})

                print(f"Returning {len(table_list)} tables for schema {schema_name}")
                return table_list

    except Exception as e:
        print(f"Error getting tables for schema {schema_name}: {e}")
        return []

def get_table_data(table_name, schema_name=None):
    """Get all data from specified table."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Get column information first
                cur.execute(sql.SQL("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """), (schema_name, table_name))

                columns_info = cur.fetchall()
                if not columns_info:
                    return None, f"Table '{schema_name}.{table_name}' not found"

                # Get actual data
                cur.execute(sql.SQL("SELECT * FROM {}.{}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name)
                ))
                rows = cur.fetchall()

                # Get column names for DataFrame
                column_names = [col[0] for col in columns_info]

                # Create DataFrame
                df = pd.DataFrame(rows, columns=column_names)

                return df, None
    except Exception as e:
        print(f"Get table data error: {e}")
        return None, str(e)

def get_table_schema(table_name, schema_name=None):
    """Get schema information for a table."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """), (schema_name, table_name))

                return cur.fetchall()
    except Exception as e:
        print(f"Get table schema error: {e}")
        return []

def create_audit_table(schema_name=None):
    """Create audit table if it doesn't exist."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {}.data_steward_audit (
                        audit_id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        username VARCHAR(255),
                        table_name VARCHAR(255) NOT NULL,
                        record_id VARCHAR(255) NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        old_value TEXT,
                        new_value TEXT,
                        action_type VARCHAR(20) DEFAULT 'UPDATE'
                    )
                """).format(sql.Identifier(schema_name)))
                conn.commit()
                return True
    except Exception as e:
        print(f"Create audit table error: {e}")
        return False

def log_audit_change(table_name, record_id, column_name, old_value, new_value, username=None, action_type='UPDATE', schema_name=None):
    """Log a single change to the audit table."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                if username is None:
                    username = os.getenv('PGUSER', 'system')

                cur.execute(sql.SQL("""
                    INSERT INTO {}.data_steward_audit
                    (username, table_name, record_id, column_name, old_value, new_value, action_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """).format(sql.Identifier(schema_name)),
                (username, table_name, str(record_id), column_name, str(old_value) if old_value is not None else None, str(new_value) if new_value is not None else None, action_type))

                conn.commit()
                return True
    except Exception as e:
        print(f"Audit logging error: {e}")
        return False

def get_username_from_request():
    """Extract username from request headers if available."""
    # In Databricks Apps, user info might be in headers
    # For now, we'll use the service principal ID
    return os.getenv('PGUSER', 'data-steward-app')

def update_table_data(table_name, updated_data, original_data, schema_name=None):
    """Update table data with changes and audit logging."""
    if schema_name is None:
        schema_name = DEFAULT_SCHEMA
    try:
        print(f"🔍 Starting update_table_data for {table_name}")
        print(f"   Updated data rows: {len(updated_data)}")
        print(f"   Original data rows: {len(original_data)}")

        # Debug: Check for new rows
        new_rows = [row for row in updated_data if '__new_row' in row and row['__new_row']]
        if new_rows:
            print(f"   Found {len(new_rows)} rows marked as new")
            for i, row in enumerate(new_rows):
                clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                print(f"     New row {i+1}: {clean_row}")

        create_audit_table()

        with get_connection() as conn:
            with conn.cursor() as cur:
                schema_info = get_table_schema(table_name)
                if not schema_info:
                    return False, "Could not get table schema"

                columns = [col[0] for col in schema_info]
                pk_column = columns[0]
                username = get_username_from_request()

                # Create lookup for original data
                original_lookup = {str(row[pk_column]): row for row in original_data}

                changes_count = 0
                inserts_count = 0
                for row in updated_data:
                    # Skip metadata fields
                    clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                    pk_value = clean_row[pk_column]
                    original_row = original_lookup.get(str(pk_value))

                    # Handle new rows (inserts) - check for new row marker or empty/missing primary key
                    is_new_row = ('__new_row' in row and row['__new_row']) or (not original_row and (pk_value == '' or pk_value is None or str(pk_value).strip() == ''))

                    if is_new_row:
                        # This is a new row to insert
                        insert_columns = [col for col in columns[1:] if col in clean_row and clean_row[col] != '' and clean_row[col] is not None]
                        if insert_columns:  # Only insert if there's actual data
                            insert_values = [clean_row[col] for col in insert_columns]

                            # Include primary key with explicit nextval() for sequence
                            all_columns = [pk_column] + insert_columns
                            sequence_name = f"{table_name}_{pk_column}_seq"
                            nextval_call = sql.SQL("nextval({})").format(sql.Literal(f"{schema_name}.{sequence_name}"))
                            placeholders = [nextval_call] + [sql.Placeholder()] * len(insert_columns)

                            # Create INSERT query with explicit nextval() for primary key
                            query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING {}").format(
                                sql.Identifier(schema_name),
                                sql.Identifier(table_name),
                                sql.SQL(", ").join([sql.Identifier(col) for col in all_columns]),
                                sql.SQL(", ").join(placeholders),
                                sql.Identifier(pk_column)
                            )

                            # Debug: Show the generated query
                            query_str = query.as_string(cur)
                            print(f"🔍 INSERT Query: {query_str}")
                            print(f"   Values: {insert_values}")

                            cur.execute(query, insert_values)
                            new_pk_result = cur.fetchone()
                            new_pk = new_pk_result[0] if new_pk_result else 'NEW'
                            inserts_count += 1

                            # Log insert to audit table
                            for col in insert_columns:
                                log_audit_change(table_name, new_pk, col, None, clean_row[col], username, action_type='INSERT', schema_name=schema_name)

                            print(f"✅ Inserted new row with {pk_column}={new_pk} into {table_name}")
                        else:
                            print(f"⚠️ Skipping new row insert - no actual data provided")
                        continue

                    if not original_row:
                        continue

                    update_clause = []
                    values = []
                    changes_made = False

                    # Compare each column
                    for col in columns[1:]:  # Skip primary key
                        if col in row:
                            old_value = original_row.get(col)
                            new_value = row[col]

                            # Check if value changed
                            if str(old_value) != str(new_value):
                                update_clause.append(f"{col} = %s")
                                values.append(new_value)
                                changes_made = True
                                changes_count += 1

                                # Log to audit table
                                log_audit_change(table_name, pk_value, col, old_value, new_value, username, schema_name=schema_name)

                    # Update if changes found
                    if update_clause and changes_made:
                        query = sql.SQL("UPDATE {}.{} SET {} WHERE {} = %s").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.SQL(", ").join([sql.SQL(clause) for clause in update_clause]),
                            sql.Identifier(pk_column)
                        )
                        values.append(pk_value)
                        cur.execute(query, values)

                # Handle deleted rows - find rows in original but not in updated
                updated_lookup = {}
                for row in updated_data:
                    clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                    if not row.get('__new_row', False) and pk_column in clean_row:
                        updated_lookup[str(clean_row[pk_column])] = clean_row
                deletes_count = 0

                for orig_row in original_data:
                    orig_pk = str(orig_row[pk_column])
                    # If original row is not in updated data, it was deleted
                    if orig_pk not in updated_lookup:
                        # Delete the row from database
                        delete_query = sql.SQL("DELETE FROM {}.{} WHERE {} = %s").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier(pk_column)
                        )
                        cur.execute(delete_query, [orig_row[pk_column]])
                        deletes_count += 1

                        # Log deletion to audit table - log each column as deleted
                        for col in columns[1:]:  # Skip primary key
                            if col in orig_row:
                                log_audit_change(table_name, orig_row[pk_column], col, orig_row[col], None, username, action_type='DELETE', schema_name=schema_name)

                        print(f"🗑️ Deleted row with {pk_column}={orig_row[pk_column]} from {table_name}")

                conn.commit()
                message_parts = []
                if inserts_count > 0:
                    message_parts.append(f"{inserts_count} new rows inserted")
                if changes_count > 0:
                    message_parts.append(f"{changes_count} changes made")
                if deletes_count > 0:
                    message_parts.append(f"{deletes_count} rows deleted")

                if message_parts:
                    return True, " and ".join(message_parts) + " - all logged"
                else:
                    return True, "No changes detected"
    except Exception as e:
        print(f"Update error: {e}")
        return False, str(e)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Add custom CSS for cell highlighting
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .ag-cell.cell-edited {
                background-color: #fff3cd !important;
                border: 2px solid #ffeeba !important;
            }
            .ag-cell.cell-saved {
                background-color: #d4edda !important;
                border: 2px solid #c3e6cb !important;
            }
            .ag-row.row-new {
                background-color: #e7f3ff !important;
                border: 2px solid #b3d9ff !important;
            }
            .ag-cell.cell-search-highlight {
                background-color: #ffff99 !important;
                border: 2px solid #ffcc00 !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Initialize audit table on app startup
try:
    create_audit_table()
    print("✅ Audit table initialized successfully")
except Exception as e:
    print(f"⚠️ Could not initialize audit table: {e}")

# App layout
app.layout = dbc.Container([
    # Header with configurable logo and title
    dbc.Row([
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    html.Img(
                        src=LOGO_URL,
                        style={
                            'height': '70px',
                            'width': '120px',
                            'marginRight': '20px',
                            'borderRadius': '8px',
                            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                            'objectFit': 'contain'
                        }
                    )
                ], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    html.H1(f"🗂️ {APP_TITLE}", className="mb-1", style={'fontSize': '2.2rem', 'fontWeight': '600'}),
                    html.P(COMPANY_NAME, className="text-muted mb-0", style={'fontSize': '16px', 'fontWeight': '500'})
                ], className="d-flex flex-column justify-content-center")
            ], className="align-items-center justify-content-center mb-4"),
            html.Hr()
        ])
    ]),

    # Schema and Table selection section
    dbc.Row([
        dbc.Col([
            html.H4("🗄️ Database Selection"),
            dbc.Row([
                dbc.Col([
                    html.Label("Schema:", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='schema-selector',
                        options=get_available_schemas(),
                        placeholder="Select a schema to begin...",
                        style={'marginBottom': '15px'}
                    )
                ], width=6),
                dbc.Col([
                    html.Label("Table:", className="fw-bold mb-2"),
                    dcc.Dropdown(
                        id='table-selector',
                        options=[],  # Will be populated based on schema selection
                        placeholder="Choose a table to manage...",
                        style={'marginBottom': '15px'}
                    )
                ], width=6)
            ]),
            html.Div(id='table-info', className="mb-3")
        ], width=12)
    ]),

    # Action buttons and status messages row
    dbc.Row([
        dbc.Col([
            html.Div(id='action-buttons')
        ], width=8),
        dbc.Col([
            html.Div(id='message-area', className="text-end")
        ], width=4)
    ], className="mb-3"),

    # Data grid section
    dbc.Row([
        dbc.Col([
            # Search and Filter Controls
            dbc.Row([
                dbc.Col([
                    dbc.InputGroup([
                        dbc.InputGroupText("🔍"),
                        dbc.Input(
                            id='search-input',
                            placeholder="Search in table data...",
                            type="text",
                            debounce=True
                        ),
                        dbc.Button("Clear", id="clear-search", color="secondary", outline=True)
                    ])
                ], width=8),
                dbc.Col([
                    dcc.Dropdown(
                        id='status-filter',
                        options=[],  # Will be populated dynamically
                        value='all',
                        placeholder="Filter by Status"
                    )
                ], width=4)
            ], className="mb-3"),
            # Result count display
            html.Div(id='result-count-display', className="mb-2"),
            html.Div(id='data-grid-container')
        ], width=12)
    ]),

    # Stores for data management
    dcc.Store(id='original-data-store'),
    dcc.Store(id='current-data-store'),
    dcc.Store(id='selected-schema-store', data=DEFAULT_SCHEMA),
    dcc.Store(id='selected-table-store'),
    dcc.Store(id='changes-pending-store', data=False),
    dcc.Store(id='edit-mode-store', data=False),
    dcc.Store(id='edited-cells-store', data={}),  # Track edited cells: {row_id: {col: True}}
    dcc.Store(id='saved-cells-store', data={}),   # Track saved cells: {row_id: {col: True}}
    dcc.Store(id='search-term-store', data=''),    # Store current search term
    dcc.Store(id='filtered-data-store'),           # Store filtered data

    # Confirmation modal for unsaved changes
    dbc.Modal([
        dbc.ModalHeader("⚠️ Unsaved Changes"),
        dbc.ModalBody([
            html.P("You have unsaved changes. What would you like to do?"),
            dbc.ButtonGroup([
                dbc.Button("Save Changes", id="save-and-continue", color="success", className="me-2"),
                dbc.Button("Discard Changes", id="discard-and-continue", color="danger")
            ])
        ])
    ], id="unsaved-changes-modal", is_open=False),

    # Confirmation modal for saving changes
    dbc.Modal([
        dbc.ModalHeader("💾 Confirm Save"),
        dbc.ModalBody([
            html.P("Are you sure you want to save these changes to the database?"),
            html.Div(id="changes-summary"),
            dbc.ButtonGroup([
                dbc.Button("Yes, Save", id="confirm-save", color="success", className="me-2"),
                dbc.Button("Cancel", id="cancel-save", color="secondary")
            ])
        ])
    ], id="save-confirmation-modal", is_open=False)

], fluid=True)

# Callback to populate tables when schema is selected
@app.callback(
    [Output('table-selector', 'options'),
     Output('table-selector', 'value'),
     Output('selected-schema-store', 'data')],
    [Input('schema-selector', 'value')],
    prevent_initial_call=True
)
def update_table_options(selected_schema):
    """Update available tables when schema is selected."""
    if not selected_schema:
        return [], None, None

    tables = get_available_tables(selected_schema)
    return tables, None, selected_schema

# Callback to handle table selection and check for unsaved changes
@app.callback(
    [Output('unsaved-changes-modal', 'is_open'),
     Output('selected-table-store', 'data')],
    [Input('table-selector', 'value'),
     Input('save-and-continue', 'n_clicks'),
     Input('discard-and-continue', 'n_clicks')],
    [State('changes-pending-store', 'data'),
     State('selected-table-store', 'data'),
     State('unsaved-changes-modal', 'is_open')],
    prevent_initial_call=True
)
def handle_table_selection(new_table, save_clicks, discard_clicks, has_changes, current_table, modal_open):
    """Handle table selection with unsaved changes warning."""
    ctx = callback_context
    if not ctx.triggered:
        return False, dash.no_update

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger == 'table-selector':
        # Check if we have unsaved changes
        if has_changes and current_table and new_table != current_table:
            return True, dash.no_update  # Open modal
        else:
            return False, new_table  # Select new table

    elif trigger == 'save-and-continue':
        # Will be handled by save callback
        return False, dash.no_update

    elif trigger == 'discard-and-continue':
        # Get the pending table selection
        return False, new_table

    return dash.no_update, dash.no_update

# Main callback to load and display table data
@app.callback(
    [Output('data-grid-container', 'children'),
     Output('table-info', 'children'),
     Output('original-data-store', 'data'),
     Output('current-data-store', 'data'),
     Output('action-buttons', 'children'),
     Output('edit-mode-store', 'data', allow_duplicate=True),
     Output('changes-pending-store', 'data', allow_duplicate=True)],
    [Input('selected-table-store', 'data')],
    [State('selected-schema-store', 'data')],
    prevent_initial_call=True
)
def load_table_data(selected_table, selected_schema):
    """Load and display data for the selected table."""
    if not selected_table:
        return [], "", None, None, [], False, False

    if not selected_schema:
        return [], "⚠️ No schema selected. Please select a schema first.", None, None, [], False, False

    # Get table data
    df, error = get_table_data(selected_table, selected_schema)

    if error:
        # Special handling for audit table not found
        if selected_table == 'data_steward_audit' and 'not found' in error.lower():
            create_audit_table(selected_schema)  # Try to create it
            return [
                dbc.Alert("📋 Audit table created. No audit records yet - make some changes to see audit logs here!", color="info")
            ], "", [], [], [], False, False
        else:
            return [
                dbc.Alert(f"Error loading table: {error}", color="danger")
            ], "", None, None, [], False, False

    if df is None or df.empty:
        if selected_table == 'data_steward_audit':
            return [
                dbc.Alert("📋 No audit records yet. Make some changes to data tables to see audit logs here!", color="info")
            ], "", [], [], [], False, False
        else:
            return [
                dbc.Alert(f"No data found in table '{selected_table}'", color="info")
            ], "", None, None, [], False, False

    # Convert DataFrame to format suitable for AG Grid
    data_dict = df.to_dict('records')

    # Create column definitions for AG Grid - read-only by default
    columns = []
    for col in df.columns:
        col_def = {
            'field': col,
            'headerName': col.replace('_', ' ').title(),
            'editable': False,  # Start in read-only mode
            'sortable': True,
            'filter': True
        }
        columns.append(col_def)

    # Special formatting for audit table
    if selected_table == 'data_steward_audit':
        # Format timestamp column
        for col_def in columns:
            if col_def['field'] == 'timestamp':
                col_def['headerName'] = '🕒 Timestamp'
            elif col_def['field'] == 'username':
                col_def['headerName'] = '👤 User'
            elif col_def['field'] == 'table_name':
                col_def['headerName'] = '📊 Table'
            elif col_def['field'] == 'column_name':
                col_def['headerName'] = '📋 Column'
            elif col_def['field'] == 'old_value':
                col_def['headerName'] = '⬅️ Old Value'
            elif col_def['field'] == 'new_value':
                col_def['headerName'] = '➡️ New Value'

    # Create AG Grid component - read-only by default
    grid = dag.AgGrid(
        id='data-grid',
        rowData=data_dict,
        columnDefs=columns,
        defaultColDef={
            'resizable': True,
            'sortable': True,
            'filter': True,
            'editable': False  # Start in read-only mode
        },
        dashGridOptions={
            'pagination': True,
            'paginationPageSize': 50,
            'domLayout': 'autoHeight'
        },
        rowClassRules={
            'row-new': 'params.data.__new_row'
        },
        style={'height': '600px'}
    )

    # Get detailed column information
    schema_info = get_table_schema(selected_table, selected_schema)

    # Create column details with formatting
    column_details = []
    for col_info in schema_info:
        col_name, data_type, is_nullable, col_default = col_info
        nullable_text = "NULL" if is_nullable == 'YES' else "NOT NULL"
        default_text = f" DEFAULT {col_default}" if col_default else ""

        column_row = html.Div([
            html.Span(col_name, style={
                'fontFamily': 'Monaco, "Lucida Console", monospace',
                'fontWeight': 'bold',
                'color': '#2E86AB',
                'marginRight': '10px'
            }),
            html.Span(data_type, style={
                'fontFamily': 'Monaco, "Lucida Console", monospace',
                'color': '#A23B72',
                'marginRight': '10px'
            }),
            html.Span(nullable_text, style={
                'fontFamily': 'Monaco, "Lucida Console", monospace',
                'color': '#F18F01',
                'fontSize': '0.85em',
                'marginRight': '5px'
            }),
            html.Span(default_text, style={
                'fontFamily': 'Monaco, "Lucida Console", monospace',
                'color': '#C73E1D',
                'fontSize': '0.85em'
            }) if default_text else ""
        ], style={'marginBottom': '4px'})
        column_details.append(column_row)

    # Table info - side by side layout with title in left column
    table_info = dbc.Card([
        dbc.CardBody([
            dbc.Row([
                # Left side - Table name and Statistics
                dbc.Col([
                    html.H5(f"📊 {selected_schema}.{selected_table.replace('_', ' ').title()}", className="card-title mb-3"),
                    html.H6("Statistics", className="mb-2", style={'color': '#495057', 'fontWeight': 'bold'}),
                    html.Div([
                        html.Strong("Records: ", style={'color': '#495057'}),
                        html.Span(f"{len(df):,}", style={'color': '#28a745', 'fontWeight': 'bold'})
                    ], className="mb-2"),
                    html.Div([
                        html.Strong("Schema: ", style={'color': '#495057'}),
                        html.Span(selected_schema, style={'color': '#17a2b8', 'fontFamily': 'Monaco, "Lucida Console", monospace'})
                    ], className="mb-2"),
                    html.Div([
                        html.Strong("Columns: ", style={'color': '#495057'}),
                        html.Span(f"{len(schema_info)}", style={'color': '#6f42c1', 'fontWeight': 'bold'})
                    ])
                ], width=4),

                # Right side - Column details
                dbc.Col([
                    html.H6("Column Details", className="mb-2", style={'color': '#495057', 'fontWeight': 'bold'}),
                    html.Div(column_details, style={
                        'backgroundColor': '#f8f9fa',
                        'padding': '10px',
                        'borderRadius': '5px',
                        'border': '1px solid #dee2e6',
                        'maxHeight': '150px',
                        'overflowY': 'auto'
                    })
                ], width=8)
            ])
        ])
    ], className="mb-3")

    # Action buttons - start in view mode (hide edit for audit table)
    if selected_table == 'data_steward_audit':
        action_buttons = [
            # Hidden buttons for callback consistency
            dbc.Button("➕ Add Row", id="add-row-button", color="info", className="me-2", style={'display': 'none'}),
            dbc.Button("✏️ Edit Data", id="edit-button", color="primary", className="me-2", style={'display': 'none'}),
            dbc.Button("💾 Save Changes", id="save-button", color="success", className="me-2", disabled=True, style={'display': 'none'}),
            html.Span("📋 Audit trail is read-only", className="text-muted ms-3", style={'fontStyle': 'italic'})
        ]
    else:
        action_buttons = [
            dbc.Button("➕ Add Row", id="add-row-button", color="info", className="me-2"),
            dbc.Button("✏️ Edit Data", id="edit-button", color="primary", className="me-2"),
            dbc.Button("💾 Save Changes", id="save-button", color="success", className="me-2", disabled=True, style={'display': 'none'}),
        ]

    return [grid], table_info, data_dict, data_dict, action_buttons, False, False

# Callback to handle edit mode toggle
@app.callback(
    [Output('edit-mode-store', 'data'),
     Output('data-grid-container', 'children', allow_duplicate=True),
     Output('action-buttons', 'children', allow_duplicate=True),
     Output('changes-pending-store', 'data', allow_duplicate=True)],
    [Input('edit-button', 'n_clicks')],
    [State('selected-table-store', 'data'),
     State('original-data-store', 'data'),
     State('edit-mode-store', 'data')],
    prevent_initial_call=True
)
def toggle_edit_mode(edit_clicks, selected_table, original_data, current_edit_mode):
    """Toggle between view and edit modes."""
    if not edit_clicks or not selected_table or not original_data:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Enter edit mode
    new_edit_mode = True

    # Recreate grid with editable columns
    columns = []

    # Add delete column for edit mode
    edit_data = original_data.copy()
    for row in edit_data:
        row['__delete_action'] = '×'

    delete_col = {
        'field': '__delete_action',
        'headerName': '🗑️',
        'width': 60,
        'editable': False,
        'sortable': False,
        'filter': False,
        'pinned': 'left',
        'cellStyle': {
            'background-color': '#dc3545',
            'color': 'white',
            'text-align': 'center',
            'font-weight': 'bold',
            'cursor': 'pointer',
            'user-select': 'none'
        }
    }
    columns.append(delete_col)

    for key in original_data[0].keys():
        col_def = {
            'field': key,
            'headerName': key.replace('_', ' ').title(),
            'editable': True,
            'sortable': True,
            'filter': True
        }
        columns.append(col_def)

    grid = dag.AgGrid(
        id='data-grid',
        rowData=edit_data,
        columnDefs=columns,
        defaultColDef={
            'resizable': True,
            'sortable': True,
            'filter': True,
            'editable': True,
            'cellClassRules': {
                'cell-edited': 'params.data.__edited && params.data.__edited[params.colDef.field]',
                'cell-saved': 'params.data.__saved && params.data.__saved[params.colDef.field]'
            }
        },
        dashGridOptions={
            'pagination': True,
            'paginationPageSize': 50,
            'domLayout': 'autoHeight'
        },
        rowClassRules={
            'row-new': 'params.data.__new_row'
        },
        style={'height': '600px'}
    )

    # Update action buttons for edit mode
    action_buttons = [
        dbc.Button("➕ Add Row", id="add-row-button", color="info", className="me-2"),
        dbc.Button("✏️ Edit Data", id="edit-button", color="primary", className="me-2", style={'display': 'none'}),
        dbc.Button("💾 Save Changes", id="save-button", color="success", className="me-2", disabled=True),
    ]

    return new_edit_mode, [grid], action_buttons, False

# Callback to track changes in the grid
@app.callback(
    [Output('current-data-store', 'data', allow_duplicate=True),
     Output('changes-pending-store', 'data', allow_duplicate=True),
     Output('save-button', 'disabled', allow_duplicate=True),
     Output('data-grid', 'rowData', allow_duplicate=True)],
    [Input('data-grid', 'cellValueChanged')],
    [State('data-grid', 'rowData'),
     State('original-data-store', 'data'),
     State('edit-mode-store', 'data')],
    prevent_initial_call=True
)
def track_data_changes(cell_changed, current_grid_data, original_data, edit_mode):
    """Track when data in the grid changes - only in edit mode."""
    # Only track changes if we're in edit mode and there was an actual cell value change event
    if not cell_changed or not edit_mode:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Mark the changed cell as edited
    if current_grid_data and cell_changed:
        row_index = cell_changed[0]['rowIndex']
        column = cell_changed[0]['colId']

        # Initialize __edited field if it doesn't exist
        if '__edited' not in current_grid_data[row_index]:
            current_grid_data[row_index]['__edited'] = {}

        # Mark this cell as edited
        current_grid_data[row_index]['__edited'][column] = True

        # Clear any saved status for this cell
        if '__saved' in current_grid_data[row_index]:
            current_grid_data[row_index]['__saved'].pop(column, None)

    # If we have a cell change event in edit mode, enable save button
    return current_grid_data, True, False, current_grid_data

# Callback to handle save button click
@app.callback(
    [Output('save-confirmation-modal', 'is_open'),
     Output('changes-summary', 'children')],
    [Input('save-button', 'n_clicks'),
     Input('confirm-save', 'n_clicks'),
     Input('cancel-save', 'n_clicks')],
    [State('current-data-store', 'data'),
     State('original-data-store', 'data'),
     State('selected-table-store', 'data'),
     State('edit-mode-store', 'data'),
     State('changes-pending-store', 'data'),
     State('save-confirmation-modal', 'is_open')],
    prevent_initial_call=True
)
def handle_save_confirmation(save_clicks, confirm_clicks, cancel_clicks, current_data, original_data, table_name, edit_mode, has_changes, modal_open):
    """Handle save confirmation modal - only in edit mode with changes."""
    ctx = callback_context
    if not ctx.triggered:
        return False, []

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]
    print(f"🔍 Save confirmation triggered by: {trigger}")

    if trigger == 'save-button':
        print(f"   Edit mode: {edit_mode}, Has changes: {has_changes}")
        print(f"   Current data rows: {len(current_data) if current_data else 0}")
        print(f"   Original data rows: {len(original_data) if original_data else 0}")
        # Only show confirmation if we're in edit mode and have current data
        if edit_mode and current_data:
            print(f"   Checking for actual changes...")
            # Check for actual changes
            actual_changes = False
            new_rows = 0

            # Create lookup for original data
            original_lookup = {}
            for row in original_data:
                clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                if clean_row:
                    pk_key = list(clean_row.keys())[0]  # First column is primary key
                    original_lookup[str(clean_row[pk_key])] = clean_row

            # Check current data for changes
            for row in current_data:
                clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                if clean_row:
                    pk_key = list(clean_row.keys())[0]  # First column is primary key
                    pk_value = clean_row[pk_key]

                    # Check if this is a new row (marked with __new_row OR empty primary key) with actual data
                    is_new_row = ('__new_row' in row and row['__new_row']) or (pk_value == '' or pk_value is None)
                    has_data = any(v != '' and v is not None for k, v in clean_row.items() if k != pk_key)

                    if is_new_row and has_data:
                        new_rows += 1
                        actual_changes = True
                    # Check if existing row has changes
                    elif str(pk_value) in original_lookup:
                        original_row = original_lookup[str(pk_value)]
                        for col, val in clean_row.items():
                            if str(original_row.get(col, '')) != str(val):
                                actual_changes = True
                                break

            # Check for deleted rows
            current_lookup = {}
            for row in current_data:
                clean_row = {k: v for k, v in row.items() if not k.startswith('__')}
                if clean_row and not row.get('__new_row', False):
                    pk_key = list(clean_row.keys())[0]  # First column is primary key
                    current_lookup[str(clean_row[pk_key])] = clean_row

            deleted_rows = 0
            for orig_row in original_data:
                orig_pk = str(orig_row[list(orig_row.keys())[0]])  # First column is primary key
                if orig_pk not in current_lookup:
                    deleted_rows += 1
                    actual_changes = True

            print(f"   Found {new_rows} new rows, {deleted_rows} deleted rows, actual_changes: {actual_changes}")
            if actual_changes:
                changes_summary = []
                summary_parts = []

                if new_rows > 0:
                    summary_parts.append(f"{new_rows} new rows")
                if deleted_rows > 0:
                    summary_parts.append(f"{deleted_rows} deleted rows")
                if actual_changes and new_rows == 0 and deleted_rows == 0:
                    summary_parts.append("modified data")

                if summary_parts:
                    action_text = " and ".join(summary_parts)
                    changes_summary.append(html.P(f"You are about to save {action_text} to the '{table_name}' table."))
                else:
                    changes_summary.append(html.P(f"You are about to save changes to rows in the '{table_name}' table."))

                print(f"   Opening save confirmation modal")
                return True, changes_summary
            else:
                print(f"   No actual changes detected")
        else:
            print(f"   Conditions not met for save confirmation")
        return False, []

    elif trigger == 'confirm-save':
        # Actual save will be handled by another callback
        return False, []

    elif trigger == 'cancel-save':
        return False, []

    return dash.no_update, dash.no_update

# Callback to actually save data to database
@app.callback(
    [Output('message-area', 'children'),
     Output('changes-pending-store', 'data', allow_duplicate=True),
     Output('original-data-store', 'data', allow_duplicate=True),
     Output('save-button', 'disabled', allow_duplicate=True),
     Output('data-grid', 'rowData', allow_duplicate=True),
     Output('action-buttons', 'children', allow_duplicate=True),
     Output('edit-mode-store', 'data', allow_duplicate=True)],
    [Input('confirm-save', 'n_clicks')],
    [State('current-data-store', 'data'),
     State('original-data-store', 'data'),
     State('selected-table-store', 'data'),
     State('selected-schema-store', 'data')],
    prevent_initial_call=True
)
def save_data_to_database(confirm_clicks, current_data, original_data, table_name, schema_name):
    """Save the modified data back to the database with audit logging."""
    if not confirm_clicks or not current_data or not original_data or not table_name:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Update the database with audit logging
    success, message = update_table_data(table_name, current_data, original_data, schema_name)

    if success:
        # Mark all edited cells as saved and clear new row status
        updated_data = []
        for row in current_data:
            new_row = row.copy()
            if '__edited' in new_row:
                # Move edited cells to saved status
                if '__saved' not in new_row:
                    new_row['__saved'] = {}
                new_row['__saved'].update(new_row['__edited'])
                new_row['__edited'] = {}  # Clear edited status

            # Clear new row highlighting after save
            if '__new_row' in new_row:
                del new_row['__new_row']

            updated_data.append(new_row)

        # Return to view mode after successful save
        action_buttons = [
            dbc.Button("➕ Add Row", id="add-row-button", color="info", className="me-2"),
            dbc.Button("✏️ Edit Data", id="edit-button", color="primary", className="me-2"),
            dbc.Button("💾 Save Changes", id="save-button", color="success", className="me-2", disabled=True, style={'display': 'none'}),
        ]

        alert = dbc.Alert(f"✅ {message}", color="success", dismissable=True)
        return alert, False, updated_data, True, updated_data, action_buttons, False  # Exit edit mode
    else:
        alert = dbc.Alert(f"❌ Error saving data: {message}", color="danger", dismissable=True)
        return alert, True, dash.no_update, False, dash.no_update, dash.no_update, dash.no_update  # Keep changes pending

# Callback to handle adding new rows
@app.callback(
    [Output('data-grid-container', 'children', allow_duplicate=True),
     Output('current-data-store', 'data', allow_duplicate=True),
     Output('changes-pending-store', 'data', allow_duplicate=True),
     Output('save-button', 'disabled', allow_duplicate=True),
     Output('edit-mode-store', 'data', allow_duplicate=True),
     Output('action-buttons', 'children', allow_duplicate=True)],
    [Input('add-row-button', 'n_clicks')],
    [State('data-grid', 'rowData'),
     State('current-data-store', 'data'),
     State('selected-table-store', 'data')],
    prevent_initial_call=True
)
def add_new_row(add_clicks, current_grid_data, current_data, table_name):
    """Add a new empty row to the table and enable edit mode."""
    if not add_clicks or not current_data or table_name == 'data_steward_audit':
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Create a new empty row based on existing data structure
    if current_data:
        # Get the structure from the first row
        sample_row = current_data[0]
        new_row = {}

        # Get the primary key column (first column)
        data_columns = [key for key in sample_row.keys() if not key.startswith('__')]
        pk_column = data_columns[0] if data_columns else None

        for key in sample_row.keys():
            if key.startswith('__'):  # Skip metadata fields
                continue
            # Set primary key to "AUTO" for new rows, others to empty string
            if key == pk_column:
                new_row[key] = 'AUTO'
            else:
                new_row[key] = ''

        # Add metadata for new row highlighting
        new_row['__new_row'] = True

        # Add the new row to the data
        updated_data = current_data.copy()
        updated_data.append(new_row)

        # Create editable grid with the new row
        columns = []

        # Add delete column for edit mode
        for row in updated_data:
            row['__delete_action'] = '×'

        delete_col = {
            'field': '__delete_action',
            'headerName': '🗑️',
            'width': 60,
            'editable': False,
            'sortable': False,
            'filter': False,
            'pinned': 'left',
            'cellStyle': {
                'background-color': '#dc3545',
                'color': 'white',
                'text-align': 'center',
                'font-weight': 'bold',
                'cursor': 'pointer',
                'user-select': 'none'
            }
        }
        columns.append(delete_col)

        for key in sample_row.keys():
            if key.startswith('__'):  # Skip metadata fields
                continue
            col_def = {
                'field': key,
                'headerName': key.replace('_', ' ').title(),
                'editable': key != pk_column,  # Make primary key non-editable for new rows
                'sortable': True,
                'filter': True
            }
            columns.append(col_def)

        grid = dag.AgGrid(
            id='data-grid',
            rowData=updated_data,
            columnDefs=columns,
            defaultColDef={
                'resizable': True,
                'sortable': True,
                'filter': True,
                'editable': True,
                'cellClassRules': {
                    'cell-edited': 'params.data.__edited && params.data.__edited[params.colDef.field]',
                    'cell-saved': 'params.data.__saved && params.data.__saved[params.colDef.field]'
                }
            },
            dashGridOptions={
                'pagination': True,
                'paginationPageSize': 50,
                'domLayout': 'autoHeight'
            },
            rowClassRules={
                'row-new': 'params.data.__new_row'
            },
            style={'height': '600px'}
        )

        # Update action buttons for edit mode
        action_buttons = [
            dbc.Button("➕ Add Row", id="add-row-button", color="info", className="me-2"),
            dbc.Button("✏️ Edit Data", id="edit-button", color="primary", className="me-2", style={'display': 'none'}),
            dbc.Button("💾 Save Changes", id="save-button", color="success", className="me-2", disabled=False),
        ]

        return [grid], updated_data, True, False, True, action_buttons  # Enable edit mode and save button

    return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Callback to populate status filter dropdown dynamically
@app.callback(
    Output('status-filter', 'options'),
    [Input('original-data-store', 'data')],
    prevent_initial_call=True
)
def update_status_filter_options(original_data):
    """Dynamically populate status filter dropdown based on actual data values."""
    if not original_data:
        return [{'label': 'All Statuses', 'value': 'all'}]

    # Extract unique status values from the data
    status_values = set()
    for row in original_data:
        if 'status' in row and row['status'] is not None:
            status_values.add(row['status'])

    # Create dropdown options
    options = [{'label': 'All Statuses', 'value': 'all'}]

    # Add options for each unique status value, sorted alphabetically
    for status in sorted(status_values):
        options.append({
            'label': f'{status} Only',
            'value': status
        })

    return options

# Callback to handle clear search button
@app.callback(
    Output('search-input', 'value'),
    [Input('clear-search', 'n_clicks')],
    prevent_initial_call=True
)
def clear_search(clear_clicks):
    """Clear the search input."""
    if clear_clicks:
        return ''
    return dash.no_update

# Callback to handle search and filtering
@app.callback(
    [Output('filtered-data-store', 'data'),
     Output('search-term-store', 'data'),
     Output('result-count-display', 'children')],
    [Input('search-input', 'value'),
     Input('status-filter', 'value'),
     Input('original-data-store', 'data')],
    prevent_initial_call=True
)
def filter_and_search_data(search_term, status_filter, original_data):
    """Filter data based on search term and status filter."""
    if not original_data:
        return [], '', html.Div()

    total_rows = len(original_data)
    filtered_data = original_data.copy()

    # Track filtering steps for display
    filter_descriptions = []

    # Apply status filter if not 'all'
    if status_filter and status_filter != 'all':
        filtered_data = [row for row in filtered_data if row.get('status') == status_filter]
        filter_descriptions.append(f"Status: {status_filter}")

    # Apply search filter
    if search_term and search_term.strip():
        search_lower = search_term.lower().strip()
        search_filtered = []

        for row in filtered_data:
            # Search in all text fields
            row_matches = False
            for key, value in row.items():
                if not key.startswith('__') and value is not None:
                    if search_lower in str(value).lower():
                        row_matches = True
                        break

            if row_matches:
                # Add search highlighting metadata
                highlighted_row = row.copy()
                highlighted_row['__search_highlight'] = {}

                for key, value in row.items():
                    if not key.startswith('__') and value is not None:
                        if search_lower in str(value).lower():
                            highlighted_row['__search_highlight'][key] = True

                search_filtered.append(highlighted_row)

        filtered_data = search_filtered
        filter_descriptions.append(f"Search: '{search_term}'")

    # Create result count display
    result_count = len(filtered_data)

    if filter_descriptions:
        filter_text = " | ".join(filter_descriptions)
        result_display = dbc.Alert([
            html.Strong(f"Showing {result_count} of {total_rows} rows"),
            html.Br(),
            html.Small(f"Filters applied: {filter_text}")
        ], color="info", className="py-2")
    elif result_count == total_rows:
        result_display = html.Div([
            html.Small(f"Showing all {total_rows} rows", className="text-muted")
        ])
    else:
        result_display = html.Div([
            html.Small(f"Showing {result_count} of {total_rows} rows", className="text-muted")
        ])

    return filtered_data, search_term or '', result_display

# Callback to update grid with filtered data
@app.callback(
    [Output('data-grid-container', 'children', allow_duplicate=True)],
    [Input('filtered-data-store', 'data'),
     Input('search-term-store', 'data')],
    [State('selected-table-store', 'data'),
     State('edit-mode-store', 'data')],
    prevent_initial_call=True
)
def update_grid_with_filtered_data(filtered_data, search_term, table_name, edit_mode):
    """Update grid display with filtered data and search highlighting."""
    if not filtered_data or table_name == 'data_steward_audit':
        return [dash.no_update]

    # Create grid with filtered data
    columns = []

    # Add delete column when in edit mode
    if edit_mode:
        # Add delete marker to each row of data
        for row in filtered_data:
            row['__delete_action'] = '×'

        delete_col = {
            'field': '__delete_action',
            'headerName': '🗑️',
            'width': 60,
            'editable': False,
            'sortable': False,
            'filter': False,
            'pinned': 'left',
            'cellStyle': {
                'background-color': '#dc3545',
                'color': 'white',
                'text-align': 'center',
                'font-weight': 'bold',
                'cursor': 'pointer',
                'user-select': 'none'
            }
        }
        columns.append(delete_col)

    for key in filtered_data[0].keys():
        if key.startswith('__'):  # Skip metadata fields
            continue

        col_def = {
            'field': key,
            'headerName': key.replace('_', ' ').title(),
            'editable': edit_mode,
            'sortable': True,
            'filter': True
        }

        # Add search highlighting if there's a search term
        if search_term:
            col_def['cellClassRules'] = {
                'cell-search-highlight': f'params.data.__search_highlight && params.data.__search_highlight["{key}"]'
            }

        columns.append(col_def)

    # Create grid
    grid = dag.AgGrid(
        id='data-grid',
        rowData=filtered_data,
        columnDefs=columns,
        defaultColDef={
            'resizable': True,
            'sortable': True,
            'filter': True,
            'editable': edit_mode
        },
        dashGridOptions={
            'pagination': True,
            'paginationPageSize': 50,
            'domLayout': 'autoHeight'
        },
        style={'height': '600px'}
    )

    return [grid]

# Callback to handle delete row clicks
@app.callback(
    [Output('current-data-store', 'data', allow_duplicate=True),
     Output('data-grid-container', 'children', allow_duplicate=True),
     Output('changes-pending-store', 'data', allow_duplicate=True),
     Output('save-button', 'disabled', allow_duplicate=True)],
    [Input('data-grid', 'cellClicked')],
    [State('current-data-store', 'data'),
     State('edit-mode-store', 'data'),
     State('search-term-store', 'data')],
    prevent_initial_call=True
)
def handle_delete_row(cell_clicked, current_data, edit_mode, search_term):
    """Handle clicking on delete column to remove a row."""
    print(f"🔍 Delete callback triggered:")
    print(f"   cell_clicked: {cell_clicked}")
    print(f"   edit_mode: {edit_mode}")
    print(f"   current_data length: {len(current_data) if current_data else 0}")

    if not cell_clicked or not edit_mode or not current_data:
        print(f"   Early return - missing data")
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    # Check if delete column was clicked
    col_id = cell_clicked.get('colId')
    print(f"   Column clicked: {col_id}")

    if col_id == '__delete_action':
        row_index = cell_clicked.get('rowIndex')

        if row_index is not None and 0 <= row_index < len(current_data):
            # Remove the clicked row
            updated_data = current_data.copy()
            deleted_row = updated_data.pop(row_index)

            # Mark as having changes
            has_changes = True

            # Recreate grid without the deleted row
            if updated_data:
                columns = []

                # Add delete column
                for row in updated_data:
                    row['__delete_action'] = '×'

                delete_col = {
                    'field': '__delete_action',
                    'headerName': '🗑️',
                    'width': 60,
                    'editable': False,
                    'sortable': False,
                    'filter': False,
                    'pinned': 'left',
                    'cellStyle': {
                        'background-color': '#dc3545',
                        'color': 'white',
                        'text-align': 'center',
                        'font-weight': 'bold',
                        'cursor': 'pointer',
                        'user-select': 'none'
                    }
                }
                columns.append(delete_col)

                # Add data columns
                for key in updated_data[0].keys():
                    if key.startswith('__'):  # Skip metadata fields
                        continue

                    col_def = {
                        'field': key,
                        'headerName': key.replace('_', ' ').title(),
                        'editable': True,
                        'sortable': True,
                        'filter': True
                    }

                    # Add search highlighting if there's a search term
                    if search_term:
                        col_def['cellClassRules'] = {
                            'cell-search-highlight': f'params.data.__search_highlight && params.data.__search_highlight["{key}"]'
                        }

                    columns.append(col_def)

                # Create grid
                grid = dag.AgGrid(
                    id='data-grid',
                    rowData=updated_data,
                    columnDefs=columns,
                    defaultColDef={
                        'resizable': True,
                        'sortable': True,
                        'filter': True,
                        'editable': True
                    },
                    dashGridOptions={
                        'pagination': True,
                        'paginationPageSize': 50,
                        'domLayout': 'autoHeight'
                    },
                    style={'height': '600px'}
                )

                return updated_data, [grid], has_changes, False  # Enable save button
            else:
                # No rows left - show empty grid
                return [], [html.Div("No data to display", className="text-center text-muted p-4")], has_changes, False  # Enable save button

    print(f"   Delete column not clicked or invalid row index")
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)