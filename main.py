import os
import json
from db.db_utils import *
from db.csv_utils import *

def load_json_data(filename):
    """Load data from a JSON file located in the 'data' directory."""
    with open(f"data/{filename}", "r") as file:
        return json.load(file)

def main():
    """Main function to handle database operations based on JSON data."""
    
    # Try to read and delete records based on saved IDs from the CSV.
    ids_with_details = read_ids_from_csv()
    if ids_with_details and delete_records_by_ids(ids_with_details):
        delete_file()

    # Load user and location data from JSON files.
    data_bank = load_json_data("bank_accounts.json")
    data_beneficiary = load_json_data("beneficiaries.json")
    data_users = load_json_data("users.json")
    data_locations = load_json_data("locations.json")
    data_financiers = load_json_data("financiers.json")
    data_offtakers = load_json_data("offtakers.json")
    data_developers = load_json_data("developers.json")
    data_projects = load_json_data("projects.json")           
    data_project_development = load_json_data("project_development_information.json")
    data_project_environmental = load_json_data("project_environmental_information.json")
    data_project_financial = load_json_data("project_financial_information.json")
    data_project_maintenance = load_json_data("project_maintenance_information.json")
    data_investor_proj_assign = load_json_data("investor_project_assignments.json")
    data_financier_payment = load_json_data("financier_payments.json")
    data_financier_receipt_details = load_json_data("financier_receipt_details.json")
    data_financier_receipt_info = load_json_data("financier_receipt_information.json")
    data_project_developer_assginment = load_json_data("project_developer_assginment.json")
    data_disbursements = load_json_data("disbursements.json")
    data_sme_project_assign = load_json_data("sme_project_assignments.json")
    data_sme_loan_repayments = load_json_data("sme_loan_repayments.json")
    data_sme_deposits = load_json_data("sme_deposits.json")
    # Prepare the location data for insertion into the database.
    locations = [
        (
            d["country"], d["city"], d["district"], d["postal_code"]
        )
        for d in data_locations
    ]
    location_id = create_locations(locations[0])
    print("this is location id", location_id)

    bank_accounts = [
        (
            d["account_name"], d["account_number"], d["account_type"],
            d["bank_name"], d["swift_code"]
        )
        for d in data_bank
    ]

    bank_acoount_id = create_bank_accounts(bank_accounts[0])
    print("this is bank accountid",bank_acoount_id)
    # Prepare the user data for insertion. Associate each user with the location ID.
    users = [
        (
            location_id, d["code"], d["is_solar_dev_role"], d["is_financier_role"],
            d["is_sponsor_role"], d["is_sme_role"], d["is_admin"], d["id_type"], d["phone"],
            d["name"], d["email"], d["contact_email"], d["password"], d["contact_phone"],
            d["profile_picture"]
        )
        for d in data_users
    ]
    
    # Insert user data and retrieve the associated user IDs.
    user_id = create_users(users[0])

    print("User ID",user_id)
    financiers = [
        (
            user_id, d["location_id"], d["wallet_address"],
            d["company_name"], d["tax_id"], d["representative_role"], d["webpage"],
            d["google_maps_location"], d["telephone"], d["profile_picture"]
        )
        for d in data_financiers
    ]   

    financier_id = create_financier(financiers[0])
    print("Financier ID:", financier_id)

    beneficiaries = [
        (
            bank_acoount_id,financier_id, d["beneficiary_name"],
            d["beneficiary_address_1"], d["beneficiary_address_2"]
        )
        for d in data_beneficiary
    ]

    beneficiary_id = create_beneficiaries(beneficiaries[0])

    print("Beneficiary ID:", beneficiary_id)

    financier_receipt_info = [
        (
            financier_id, d["name"], d["tax_id"],
            d["address"], d["email"], d["phone"]
        )
        for d in data_financier_receipt_info
    ]

    financier_receipt_info_id = create_financier_receipt_info(financier_receipt_info[0])
    print("Financier Receipt Info ID:", financier_receipt_info_id)

    offtakers = [
        (
            user_id, d["industry"], d["description"], d["annual_sales"]
        )
        for d in data_offtakers
    ]

    offtaker_id = create_offtaker(offtakers[0])
    print("Offtaker ID:", offtaker_id)

    developers = [
        (
            user_id, d["capacity_history"], d["years_experience"], d["description"]
        )
        for d in data_developers
    ]

    developer_id = create_developer(developers[0])

    print("Developer ID:", developer_id)



    projects  = [
        (
            location_id, d["name"], d["description"], d["nominal_capacity"],
            d["estimated_annual_energy"], d["stage"], d["type"], d["technology"],
            d["power_plant_amount"], d["days_to_participate"], d["num_sponsors"],
            d["num_financiers"], d["ppa_duration"], d["admin_listing_date"],
            d["financing_stage"]
        )
        for d in data_projects
    ]
    
    project_id = create_projects(projects[0])
    print("Project ID:", project_id)


    projects_development = [
        (
            project_id, d["interest_rate"], d["panel_count"], d["panel_brand"],
            d["energy_cost_annual"], d["energy_maintenance_cost_annual"], d["energy_tariff"],
            d["projected_income"], d["maintenance_cost_annual"], d["insurance_cost_annual"],
            d["drex_cost_annual"], d["radiation_onsite"], d["technical_memo"],
            d["technical_blueprint"], d["technical_equipment_specs"], d["operation_estimate"],
            d["construction_starts_estimate"], d["legal_document"], d["commercial_offer"],
            d["intention_letter"], d["insurance_funding"], d["insurance_completion"],
            d["insurance_all_risks"], d["insurance_energy_generation"], 
            d["insurance_assets_services"]
        )
        for d in data_project_development
    ]

    project_development_id = create_project_development_information(projects_development[0])
    print("Project Development ID:", project_development_id)

    project_env = [
        (
            project_id, d["trees_planted_estimated"], d["co2_removed_stimated"]
        )
        for d in data_project_environmental
    ]

    project_env_id = create_project_environmental_information(project_env[0])
    print("Project Environmental ID:", project_env_id)

    project_finance = [
        (
          project_id, d["energy_fee"], d["success_fee"], d["cost"],
            d["financier_rate"], d["sponsor_rate"], d["financier_amount"],
            d["sponsor_amount"], d["financier_raised"], d["sponsor_raised"],
            d["aggregate_raised"], d["debt_duration"], d["estimated_annual_return"]
        )
        for d in data_project_financial
    ]

    project_finance_id = create_project_financial_information(project_finance[0])
    print("Project Finance ID:", project_finance_id)

    project_maintenance = [
        (
            project_id, d["name"], d["date"], d["description"],
            d["provider"], d["pictures"], d["report"]
        )
        for d in data_project_maintenance
    ]

    project_maintenance_id = create_project_maintenance_information(project_maintenance[0])
    print("Project Maintenance ID:", project_maintenance_id)


    investor_project_assign = [
        (
            project_id, financier_id, d["interest_date"],
            d["commitment_level"], d["assigned_date"]
        )
        for d in data_investor_proj_assign
    ]

    investor_project_assign_id = create_investor_project_assignments(investor_project_assign[0])
    print("Investor Project Assign ID:", investor_project_assign_id)

    financier_payments = [
        (
            investor_project_assign_id, d["amount"], d["order"], d["payment_date"]
        )
        for d in data_financier_payment
    ]

    financier_payment_id = create_financier_payments(financier_payments[0])
    print("Financier Payments ID:", financier_payment_id)

    financier_receipt_details = [
        (
            investor_project_assign_id, d["transfer_id"], d["transferred_payment_timestamp"],
            d["registered_payment_timestamp"], d["expected_payment_timestamp"], d["amount"],
            d["state"], d["pdf"], d["bank_fee"]
        )
        for d in data_financier_receipt_details
    ]

    financier_receipt_details_id = create_financier_receipt_details(financier_receipt_details[0])
    print("Financier Receipt Details ID:", financier_receipt_details_id)

    project_developer_assignments = [
        (
            project_id,developer_id,
            d["assigned_date_time"], d["disbursement_count"]
        )
        for d in data_project_developer_assginment
    ]

    project_developer_assignments_id = create_project_developer_assignments(project_developer_assignments[0])
    print("Project Developer Assignments ID:", project_developer_assignments_id)

    disbursements = [   
        (
            project_developer_assignments_id, d["amount"], d["status"],
            d["description"], d["date"]
        )
        for d in data_disbursements
    ]

    disbursements_id = create_disbursement(disbursements[0])
    print("Disbursements ID:", disbursements_id)


    sme_project_assignments = [
        (
            project_id,offtaker_id,
            d["energy_consumption_annual"], d["ppa_discount"], d["total_owed_amount"],
            d["loan_repayment_count"]
        )
        for d in data_sme_project_assign
    ]
    sme_project_assignment_id = create_sme_project_assignment(sme_project_assignments[0])
    print("SME Project Assignment ID:", sme_project_assignment_id)

    
    sme_deposits = [
        (
            sme_project_assignment_id, d["amount"],
            d["remaining"], d["status"], d["date"]
        )
        for d in data_sme_deposits
    ]

    sme_deposit_id = create_sme_deposit(sme_deposits[0])
    print("SME Deposits ID:", sme_deposit_id)


    sme_loan_repayments = [
        (
            sme_project_assignment_id, d["progress"],
            d["sme_deposit_energy"]
        )
        for d in data_sme_loan_repayments
    ]

    sme_loan_repayment_id = create_sme_loan_repayment(sme_loan_repayments[0])

    print("Sme loan repayment Id", sme_loan_repayment_id)

    # Save the returned user and location IDs to a CSV file.
    save_ids_to_csv(user_id, "user_id", "users") 
    save_ids_to_csv(location_id, "location_id", "locations")
    save_ids_to_csv(financier_id, "financier_id", "financiers")
    save_ids_to_csv(offtaker_id, "offtaker_id", "offtakers")
    save_ids_to_csv(developer_id, "developer_id","developers")

if __name__ == "__main__":
    main()
