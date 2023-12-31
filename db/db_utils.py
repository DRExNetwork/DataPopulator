import psycopg2
from config import db_config


def connect():
    """Establish a connection to the database using provided configurations."""
    try:
        connection = psycopg2.connect(
            database=db_config.DB_NAME,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD,
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
        )
        return connection
    except Exception as e:
        print("Error connecting to the database:", e)


def generic_insert(sql, data, multiple):
    """Generic method to insert data into the database. Returns the IDs of the inserted rows."""
    con = connect()
    cur = con.cursor()

    if multiple:
        ids = []
        for item in data:
            cur.execute(sql, item)
            ids.append(cur.fetchone()[0])
        result = ids
    else:
        x = cur.execute(sql, data)
        print(x)
        result = cur.fetchone()[0]  # Retrieve the ID of the inserted row.
        # print(result)

    con.commit()
    cur.close()
    con.close()
    return result


def delete_records_by_ids(ids_with_details):
    """Delete records from the database based on provided IDs and details."""
    con = connect()
    cur = con.cursor()
    all_deleted = True  # Indicator to check if all records were deleted successfully.
    for id, column_name, table_name in ids_with_details:
        try:
            cur.execute(f"DELETE FROM {table_name} WHERE {column_name} = %s", (id,))
            print(
                f"Record with {column_name} {id} from table {table_name} deleted successfully."
            )
        except Exception as e:
            print(
                f"Failed to delete record with {column_name} {id} from table {table_name}. Error: {e}"
            )
            all_deleted = False  # If an error occurred, set the indicator to False.

    con.commit()
    cur.close()
    con.close()
    return all_deleted


# The following functions handle specific table operations.
# They typically define the SQL command for the operation and then use the generic_insert method.


def create_locations(list, multiple=False):
    """Insert data into the 'locations' table."""
    sql = """
    INSERT INTO public.locations (
        country, city, district, postal_code
    ) VALUES (%s, %s, %s, %s) RETURNING location_id
    """

    return generic_insert(sql, list, multiple)


def create_users(list, multiple=False):  # need to write a update function
    """Insert data into the 'users' table."""
    sql = """
    INSERT INTO public.users (
        location_id, uid, is_solar_dev_role, is_financier_role, 
        is_sponsor_role, is_sme_role, is_admin, id_type, phone, 
        name, email, contact_email, password, contact_phone, 
        profile_picture
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING user_id
    """

    return generic_insert(sql, list, multiple)


def create_financier(list, multiple=False):
    """Insert data into the 'financiers' table."""
    sql = """
    INSERT INTO public.financiers (
        user_id, location_id,  wallet_address, 
        company_name, tax_id, representative_role, webpage,
        google_maps_location, telephone, profile_picture
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING financier_id
    """
    return generic_insert(sql, list, multiple)


def create_offtaker(list, multiple=False):
    """Insert data into the 'offtakers' table."""
    sql = """
    INSERT INTO public.offtakers (
        user_id, industry, description, annual_sales
    ) VALUES (%s, %s, %s, %s) RETURNING sme_id
    """
    return generic_insert(sql, list, multiple)


def create_developer(list, multiple=False):
    """Insert data into the 'developers' table."""
    sql = """
    INSERT INTO public.developers (
        user_id, capacity_history, years_experience, description
    ) VALUES (%s, %s, %s, %s) RETURNING developer_id
    """
    return generic_insert(sql, list, multiple)


def create_beneficiaries(list, multiple=False):
    sql = """
    INSERT INTO public.beneficiaries (
        bank_account_id, financier_id, beneficiary_name,
        beneficiary_address_1, beneficiary_address_2
    ) VALUES (%s, %s, %s, %s, %s) RETURNING beneficiary_id;
    """

    return generic_insert(sql, list, multiple)


def create_bank_accounts(list, multiple=False):
    sql = """
    INSERT INTO public.bank_accounts (
        account_name, account_number, account_type,
        bank_name, swift_code
    ) VALUES (%s, %s, %s, %s, %s) RETURNING bank_account_id;
    """

    return generic_insert(sql, list, multiple)


def create_investor_project_assignments(list, multiple=False):
    sql = """
    INSERT INTO public.investor_project_assignments (
        project_id, financier_id, interest_date,
        commitment_level, assigned_date
    ) VALUES (%s, %s, %s, %s, %s) RETURNING investor_assignment_id;
    """

    return generic_insert(sql, list, multiple)


def create_financier_payments(list, multiple=False):
    sql = """
    INSERT INTO public.financier_payments (
        investor_assignment_id, amount, "order", payment_date
    ) VALUES (%s, %s, %s, %s) RETURNING financier_payment_id;
    """

    return generic_insert(sql, list, multiple)


def create_financier_receipt_info(list, multiple=False):
    """Insert data into the 'financier_details' table."""
    sql = """
    INSERT INTO public.financier_receipt_information (
        financier_id, name, tax_id,
        address, email, phone
    ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING financier_id
    """
    return generic_insert(sql, list, multiple)


def create_financier_receipt_details(list, multiple=False):
    sql = """
    INSERT INTO public.financier_receipt_details (
        investor_assignment_id, transfer_id, transferred_payment_timestamp,
        registered_payment_timestamp, expected_payment_timestamp, amount,
        state, pdf, bank_fee
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING financier_receipt_details_id;
    """

    return generic_insert(sql, list, multiple)


def create_projects(list, multiple=False):
    sql = """
    INSERT INTO public.projects (
        location_id, name, description, nominal_capacity,
        estimated_annual_energy, stage, type, technology,
        power_plant_amount, days_to_participate, num_sponsors,
        num_financiers, ppa_duration, admin_listing_date, financing_stage
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING project_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_development_information(list, multiple=False):
    sql = """
    INSERT INTO public.project_development_information (
        project_id, interest_rate, panel_count, panel_brand,
        energy_cost_annual, energy_maintenance_cost_annual, energy_tariff,
        projected_income, maintenance_cost_annual, insurance_cost_annual,
        drex_cost_annual, radiation_onsite, technical_memo,
        technical_blueprint, technical_equipment_specs, operation_estimate,
        construction_starts_estimate, legal_document, commercial_offer,
        intention_letter, insurance_funding, insurance_completion,
        insurance_all_risks, insurance_energy_generation, insurance_assets_services
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING project_development_information_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_maintenance_information(list, multiple=False):
    sql = """
    INSERT INTO public.project_maintenance_information (
        project_id, name, date, description,
        provider, pictures, report
    ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING maintenance_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_environmental_information(list, multiple=False):
    sql = """
    INSERT INTO public.project_environmental_information (
        project_id, trees_planted_estimated, co2_removed_stimated
    ) VALUES (%s, %s, %s) RETURNING project_environmental_information_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_environmental_information_graph(list, multiple=False):
    sql = """
    INSERT INTO public.project_environmental_information_graph (
        project_environmental_information_id, trees_planted_actual, co2_removed_actual,
        trees_planted_actual_timestamp, co2_removed_actual_timestamp
    ) VALUES (%s, %s, %s, %s, %s) RETURNING project_environmental_information_graph_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_financial_information(list, multiple=False):
    sql = """
    INSERT INTO public.project_financial_information (
        project_id, energy_fee, success_fee, cost,
        financier_rate, sponsor_rate, financier_amount,
        sponsor_amount, financier_raised, sponsor_raised,
        aggregate_raised, debt_duration, estimated_annual_return
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING project_financial_information_id;
    """

    return generic_insert(sql, list, multiple)


def create_spvs(list, multiple=False):
    sql = """
    INSERT INTO public.spvs (
        project_id, bank_account_id, name,
        payment_method, country, account_name
    ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING spv_id;
    """

    return generic_insert(sql, list, multiple)


def create_project_developer_assignments(list, multiple=False):
    """Insert data into the 'project_developer_assignments' table."""
    sql = """
    INSERT INTO public.project_developer_assignments (
        project_id, developer_id,
        assigned_date_time, disbursement_count
    ) VALUES (%s, %s, %s, %s) RETURNING developer_assignment_id
    """
    return generic_insert(sql, list, multiple)


def create_disbursement(list, multiple=False):
    """Insert data into the 'disbursements' table."""
    sql = """
    INSERT INTO public.disbursements (
        developer_assignment_id, amount, status,
        description, date
    ) VALUES (%s, %s, %s, %s, %s) RETURNING disbursement_id
    """
    return generic_insert(sql, list, multiple)


def create_sme_project_assignment(list, multiple=False):
    """Insert data into the 'sme_project_assignments' table."""
    sql = """
    INSERT INTO public.sme_project_assignments (
         project_id, sme_id,
        energy_consumption_annual, ppa_discount, total_owed_amount,
        loan_repayment_count
    ) VALUES ( %s, %s, %s, %s, %s, %s) RETURNING sme_project_id
    """
    return generic_insert(sql, list, multiple)


def create_sme_deposit(list, multiple=False):
    """Insert data into the 'sme_deposits' table."""
    sql = """
    INSERT INTO public.sme_deposits (
         sme_project_id, amount,
        remaining, status, date
    ) VALUES ( %s, %s, %s, %s, %s) RETURNING deposit_id
    """
    return generic_insert(sql, list, multiple)


def create_sme_loan_repayment(list, multiple=False):
    """Insert data into the 'sme_loan_repayments' table."""
    sql = """
    INSERT INTO public.sme_loan_repayments (
         sme_project_id, progress,
        sme_deposit_energy
    ) VALUES  (%s, %s, %s) RETURNING loan_repayment_id
    """
    generic_insert(sql, list, multiple)


def create_auth_users(data, multiple=False):
    """Insert data into the 'auth.users' table."""
    sql = """
    INSERT INTO
  auth.users (
    id,
    instance_id,
    ROLE,
    aud,
    email,
    raw_app_meta_data,
    raw_user_meta_data,
    is_super_admin,
    encrypted_password,
    created_at,
    updated_at,
    last_sign_in_at,
    email_confirmed_at,
    confirmation_sent_at,
    confirmation_token,
    recovery_token,
    email_change_token_new,
    email_change
  )
VALUES
  (
    gen_random_uuid(),
    '00000000-0000-0000-0000-000000000000',
    'authenticated',
    'authenticated',
    'devshobhits@email.io',
    '{"provider":"email","providers":["email"]}',
    '{}',
    FALSE,
    crypt('Pa55word!', gen_salt('bf')),
    NOW(),
    NOW(),
    NOW(),
    NOW(),
    NOW(),
    '',
    '',
    '',
    ''
  ) RETURNING id;
    """
    # Execute the SQL command here
    generic_insert(sql, data, multiple)


def create_auth_identities(data, multiple=False):
    """Insert data into the 'auth.identities' table."""
    sql = """
INSERT INTO
  auth.identities (
    id,
    provider,
    user_id,
    identity_data,
    last_sign_in_at,
    created_at,
    updated_at
  )
VALUES
  (
    (
      SELECT
        id
      FROM
        auth.users
      WHERE
        email = 'devshobhits@email.io'
    ),
    'email',
    (
      SELECT
        id
      FROM
        auth.users
      WHERE
        email = 'devshobhits@email.io'
    ),
    json_build_object(
      'sub',
      (
        SELECT
          id
        FROM
          auth.users
        WHERE
          email = 'dev@email.io'
      )
    ),
    NOW(),
    NOW(),
    NOW()
  ) RETURNING id;
    """
    generic_insert(sql, data, multiple)
    # Execute the SQL command here
